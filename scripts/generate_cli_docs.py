#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Alex Sizykh

"""Generate Docusaurus CLI command reference pages from `orbit --help` output."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ORBIT_BIN = REPO_ROOT / "target" / "debug" / "orbit"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "website" / "docs" / "cli" / "commands"


@dataclass
class CommandDoc:
    path: tuple[str, ...]
    description: str
    usage: str
    sections: dict[str, list[tuple[str, str]]]
    subcommands: list[str]
    subcommand_descriptions: dict[str, str]


def run_checked(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=True)


def command_tokens(path: tuple[str, ...]) -> tuple[str, ...]:
    return ("orbit", *path)


def command_label(path: tuple[str, ...]) -> str:
    return " ".join(command_tokens(path))


def command_slug(path: tuple[str, ...]) -> str:
    return "-".join(command_tokens(path))


def section_tokens(docs: list[CommandDoc]) -> set[str]:
    tokens: set[str] = set()
    for doc in docs:
        if len(doc.path) > 1:
            tokens.add(doc.path[0])
    return tokens


def file_path_for_doc(path: tuple[str, ...], sections: set[str]) -> Path:
    filename = f"{command_slug(path)}.md"
    if path and path[0] in sections:
        return Path(path[0]) / filename
    return Path(filename)


def doc_link(
    from_path: tuple[str, ...],
    to_path: tuple[str, ...],
    doc_paths: dict[tuple[str, ...], Path],
) -> str:
    from_file = doc_paths[from_path]
    to_file = doc_paths[to_path]
    relative = Path(os.path.relpath(to_file, start=from_file.parent)).as_posix()
    if relative.endswith(".md"):
        relative = relative[:-3]
    if not relative.startswith(".") and not relative.startswith(".."):
        return f"./{relative}"
    return relative


def strip_banner(help_text: str) -> str:
    lines = help_text.splitlines()
    has_non_ascii_header = bool(
        lines and any(any(ord(char) > 127 for char in line) for line in lines[:2])
    )
    if has_non_ascii_header:
        idx = 0
        while idx < len(lines) and lines[idx].strip():
            idx += 1
        while idx < len(lines) and not lines[idx].strip():
            idx += 1
        lines = lines[idx:]
    return "\n".join(lines).strip()


def extract_description(help_text: str) -> str:
    description_lines: list[str] = []
    for line in help_text.splitlines():
        if line.startswith("Usage:"):
            break
        if line.strip():
            description_lines.append(line.strip())
    return " ".join(description_lines).strip()


def parse_sections(help_text: str) -> tuple[str, dict[str, list[tuple[str, str]]]]:
    usage = ""
    sections: dict[str, list[tuple[str, str]]] = {}
    current_section: str | None = None
    current_item_index: int | None = None

    for line in help_text.splitlines():
        stripped = line.strip()

        if not usage and stripped.startswith("Usage:"):
            usage = stripped[len("Usage:") :].strip()
            current_section = None
            current_item_index = None
            continue

        section_match = re.match(r"^([A-Z][A-Za-z ]*):$", stripped)
        if section_match:
            current_section = section_match.group(1)
            sections[current_section] = []
            current_item_index = None
            continue

        if current_section is None:
            continue

        if not stripped:
            current_item_index = None
            continue

        item_with_description = re.match(r"^ {2,}(\S.*?)\s{2,}(.*)$", line)
        if item_with_description:
            item = item_with_description.group(1).strip()
            description = item_with_description.group(2).strip()
            sections[current_section].append((item, description))
            current_item_index = len(sections[current_section]) - 1
            continue

        item_without_description = re.match(r"^ {2,}(\S.*?)\s*$", line)
        if item_without_description and current_item_index is None:
            item = item_without_description.group(1).strip()
            sections[current_section].append((item, ""))
            current_item_index = len(sections[current_section]) - 1
            continue

        continuation = re.match(r"^ {2,}(.*)$", line)
        if continuation and current_item_index is not None:
            continuation_text = continuation.group(1).strip()
            if continuation_text:
                starts_new_item = bool(
                    re.match(r"^(-{1,2}[A-Za-z0-9][A-Za-z0-9-]*|<[A-Za-z0-9_-]+>)", continuation_text)
                )
                if starts_new_item:
                    sections[current_section].append((continuation_text, ""))
                    current_item_index = len(sections[current_section]) - 1
                    continue
                item, description = sections[current_section][current_item_index]
                merged = (
                    f"{description} {continuation_text}".strip()
                    if description
                    else continuation_text
                )
                sections[current_section][current_item_index] = (item, merged)

    return usage, sections


def collect_docs(orbit_bin: Path) -> list[CommandDoc]:
    queue: list[tuple[str, ...]] = [tuple()]
    seen: set[tuple[str, ...]] = {tuple()}
    docs: list[CommandDoc] = []

    while queue:
        path = queue.pop(0)
        result = run_checked([str(orbit_bin), *path, "--help"], cwd=REPO_ROOT)
        cleaned_help = strip_banner(result.stdout)
        description = extract_description(cleaned_help)
        usage, sections = parse_sections(cleaned_help)
        command_items = sections.get("Commands", [])
        subcommands = [name for name, _ in command_items if name != "help"]
        subcommand_descriptions = {
            name: details for name, details in command_items if name != "help"
        }

        docs.append(
            CommandDoc(
                path=path,
                description=description,
                usage=usage,
                sections=sections,
                subcommands=subcommands,
                subcommand_descriptions=subcommand_descriptions,
            )
        )

        for subcommand in subcommands:
            child = (*path, subcommand)
            if child not in seen:
                seen.add(child)
                queue.append(child)

    docs.sort(key=lambda item: (len(item.path), item.path))
    return docs


def yaml_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def render_doc(doc: CommandDoc, doc_paths: dict[tuple[str, ...], Path]) -> str:
    title = command_label(doc.path)
    description = doc.description or (
        "Top-level help for the orbit CLI" if not doc.path else f"CLI help for {title}"
    )

    lines: list[str] = [
        "---",
        f"title: {yaml_quote(title)}",
        f"description: {yaml_quote(description)}",
        "---",
    ]

    if doc.description:
        lines.extend(["", doc.description])

    if doc.path:
        parent_path = doc.path[:-1]
        parent_title = command_label(parent_path)
        parent_link = doc_link(doc.path, parent_path, doc_paths)
        lines.extend(["", f"Parent command: [`{parent_title}`]({parent_link})"])

    if doc.usage:
        lines.extend(["", f"**Usage:** `{doc.usage}`"])

    preferred_order = ["Arguments", "Options", "Commands"]
    ordered_sections = preferred_order + [
        section_name
        for section_name in doc.sections
        if section_name not in preferred_order
    ]

    for section_name in ordered_sections:
        items = doc.sections.get(section_name, [])
        if not items:
            continue

        heading = section_name
        if section_name == "Options":
            heading = "Flags and Options"
        elif section_name == "Commands":
            heading = "Subcommands"

        lines.extend(["", f"## {heading}", ""])

        for item, details in items:
            if section_name == "Commands" and item == "help":
                continue
            if section_name == "Commands" and item != "help":
                child_path = (*doc.path, item)
                child_title = command_label(child_path)
                child_link = doc_link(doc.path, child_path, doc_paths)
                if details:
                    lines.append(f"- [`{child_title}`]({child_link}): {details}")
                else:
                    lines.append(f"- [`{child_title}`]({child_link})")
                continue

            label = f"`{item}`"
            if details:
                lines.append(f"- {label}: {details}")
            else:
                lines.append(f"- {label}")

    #lines.extend(
    #    [
    #        "",
    #        "_Generated from `--help` output by `python3 scripts/generate_cli_docs.py`._",
    #        "",
    #    ]
    #)

    return "\n".join(lines)


def write_docs(docs: list[CommandDoc], output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sections = section_tokens(docs)
    doc_paths = {doc.path: file_path_for_doc(doc.path, sections) for doc in docs}

    root_doc = next(doc for doc in docs if not doc.path)
    ordered_sections = [token for token in root_doc.subcommands if token in sections]

    for index, section in enumerate(ordered_sections, start=1):
        section_dir = output_dir / section
        section_dir.mkdir(parents=True, exist_ok=True)
        section_doc_path = doc_paths[(section,)].with_suffix("").as_posix()
        section_category = {
            "label": f"orbit {section}",
            "position": index,
            "link": {
                "type": "doc",
                "id": f"cli/commands/{section_doc_path}",
            },
        }
        (section_dir / "_category_.json").write_text(
            f"{json.dumps(section_category, indent=2)}\n",
            encoding="utf-8",
        )

    for doc in docs:
        out_file = output_dir / doc_paths[doc.path]
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(render_doc(doc, doc_paths), encoding="utf-8")

    category_file = output_dir / "_category_.json"
    root_category = {"label": "Command Docs", "position": 2}
    category_file.write_text(
        f"{json.dumps(root_category, indent=2)}\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CLI docs pages from orbit --help output"
    )
    parser.add_argument(
        "--orbit-bin",
        type=Path,
        default=DEFAULT_ORBIT_BIN,
        help="Path to the orbit binary (defaults to target/debug/orbit)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Where generated markdown files are written",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip `cargo build -p orbit` before generation",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    orbit_bin = args.orbit_bin.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()

    try:
        if not args.skip_build:
            run_checked(["cargo", "build", "-p", "orbit"], cwd=REPO_ROOT)

        if not orbit_bin.exists():
            print(
                f"error: orbit binary was not found at {orbit_bin}. "
                "Run without --skip-build or pass --orbit-bin.",
                file=sys.stderr,
            )
            return 1

        docs = collect_docs(orbit_bin)
        write_docs(docs, output_dir)
        print(f"Generated {len(docs)} command docs into {output_dir}")
        return 0
    except subprocess.CalledProcessError as error:
        if error.stdout:
            print(error.stdout, file=sys.stderr)
        if error.stderr:
            print(error.stderr, file=sys.stderr)
        print(f"error: command failed: {' '.join(error.cmd)}", file=sys.stderr)
        return error.returncode or 1


if __name__ == "__main__":
    raise SystemExit(main())
