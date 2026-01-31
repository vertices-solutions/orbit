#!/usr/bin/env python3

# Workhorse for coverage reporting
import json
from pathlib import Path


def find_report(dir_path: Path) -> Path:
    preferred = dir_path / "tarpaulin-report.json"
    if preferred.exists():
        return preferred
    candidates = sorted(dir_path.glob("*.json"))
    if candidates:
        return candidates[0]
    raise FileNotFoundError(f"no JSON report found in {dir_path}")


def to_float(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def extract_coverage(data):
    for key in ("line_coverage", "coverage", "percent_covered", "percent_coverage"):
        val = to_float(data.get(key))
        if val is not None:
            total = data.get("total")
            if total is None:
                total = data.get("coverable")
            return val, data.get("covered"), total

    coverage = data.get("coverage")
    if isinstance(coverage, dict):
        for key in ("line_coverage", "coverage", "percent", "percent_covered"):
            val = to_float(coverage.get(key))
            if val is not None:
                return (
                    val,
                    coverage.get("covered", data.get("covered")),
                    coverage.get("total", data.get("total") or data.get("coverable")),
                )

        lines = coverage.get("lines") or coverage.get("line")
        if isinstance(lines, dict):
            val = to_float(lines.get("percent") or lines.get("coverage") or lines.get("line_coverage"))
            covered = lines.get("covered")
            total = lines.get("total")
            if val is not None:
                return val, covered, total
            if covered is not None and total:
                return covered / total * 100.0, covered, total

    lines = data.get("lines")
    if isinstance(lines, dict):
        val = to_float(lines.get("percent") or lines.get("coverage") or lines.get("line_coverage"))
        covered = lines.get("covered")
        total = lines.get("total")
        if val is not None:
            return val, covered, total
        if covered is not None and total:
            return covered / total * 100.0, covered, total

    covered = data.get("covered")
    total = data.get("total")
    if total is None:
        total = data.get("coverable")
    if covered is not None and total:
        try:
            return float(covered) / float(total) * 100.0, covered, total
        except Exception:
            pass

    files = data.get("files")
    if isinstance(files, dict) or isinstance(files, list):
        covered_sum = 0
        total_sum = 0
        iterable = files.values() if isinstance(files, dict) else files
        for file_data in iterable:
            if not isinstance(file_data, dict):
                continue
            covered = file_data.get("covered")
            total = file_data.get("total")
            if total is None:
                total = file_data.get("coverable")
            uncovered = file_data.get("uncovered")
            if covered is not None and uncovered is not None:
                covered_sum += covered
                total_sum += covered + uncovered
            elif covered is not None and total is not None:
                covered_sum += covered
                total_sum += total
        if total_sum:
            return covered_sum / total_sum * 100.0, covered_sum, total_sum

    return None, None, None


def load_member(member_name: str):
    report_dir = Path("target/tarpaulin") / member_name
    report_path = find_report(report_dir)
    data = json.loads(report_path.read_text())
    return extract_coverage(data)


def main() -> int:
    members = ["orbit", "orbitd"]
    rows = []
    for member in members:
        percent, covered, total = load_member(member)
        coverage_str = "n/a" if percent is None else f"{percent:.2f}%"
        lines_str = f"{covered}/{total}" if covered is not None and total is not None else "-"
        rows.append((member, coverage_str, lines_str))

    headers = ("Member", "Coverage", "Lines")
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(str(cell))) for width, cell in zip(widths, row)]

    def format_row(cells):
        return "  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(cells))

    print(format_row(headers))
    print(format_row(tuple("-" * width for width in widths)))
    for row in rows:
        print(format_row(row))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
