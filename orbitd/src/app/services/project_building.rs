// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

use tar::Builder;
use walkdir::WalkDir;
use zstd::stream::{Decoder, Encoder};

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};

pub fn create_tarball(
    source_root: &Path,
    root_name: &str,
    tarball_path: &Path,
    include_git: bool,
) -> AppResult<()> {
    if !source_root.is_dir() {
        return Err(invalid_argument(format!(
            "blueprint root '{}' is not a directory",
            source_root.display()
        )));
    }
    if let Some(parent) = tarball_path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            local_error(format!(
                "failed to create tarball directory {}: {err}",
                parent.display()
            ))
        })?;
    }

    let file = File::create(tarball_path).map_err(|err| {
        local_error(format!(
            "failed to create tarball {}: {err}",
            tarball_path.display()
        ))
    })?;
    let encoder = Encoder::new(file, 0)
        .map_err(|err| local_error(format!("failed to start tarball encoder: {err}")))?;
    let mut builder = Builder::new(encoder);

    let mut entries: Vec<(PathBuf, bool)> = Vec::new();
    for entry in WalkDir::new(source_root).follow_links(false) {
        let entry =
            entry.map_err(|err| local_error(format!("failed to walk blueprint root: {err}")))?;
        let path = entry.path();
        let rel = path
            .strip_prefix(source_root)
            .map_err(|_| local_error("failed to compute tarball relative path".to_string()))?;
        if rel.as_os_str().is_empty() {
            continue;
        }
        if !include_git && rel.components().any(|c| c.as_os_str() == ".git") {
            continue;
        }
        if entry.file_type().is_symlink() {
            continue;
        }
        if entry.file_type().is_dir() || entry.file_type().is_file() {
            entries.push((rel.to_path_buf(), entry.file_type().is_dir()));
        }
    }

    entries.sort_by_key(|(rel, _)| rel.to_string_lossy().to_string());

    builder
        .append_dir(root_name, source_root)
        .map_err(|err| local_error(format!("failed to write tarball root: {err}")))?;

    for (rel, is_dir) in entries {
        let dest = Path::new(root_name).join(&rel);
        let src = source_root.join(&rel);
        if is_dir {
            builder
                .append_dir(dest, src)
                .map_err(|err| local_error(format!("failed to add directory: {err}")))?;
        } else {
            builder
                .append_path_with_name(src, dest)
                .map_err(|err| local_error(format!("failed to add file: {err}")))?;
        }
    }

    let encoder = builder
        .into_inner()
        .map_err(|err| local_error(format!("failed to finalize tarball: {err}")))?;
    encoder
        .finish()
        .map_err(|err| local_error(format!("failed to finish tarball: {err}")))?;
    Ok(())
}

pub fn blake3_file_hash(path: &Path) -> AppResult<String> {
    let file = File::open(path)
        .map_err(|err| local_error(format!("failed to open file {}: {err}", path.display())))?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_reader(&mut reader)
        .map_err(|err| local_error(format!("failed to hash file {}: {err}", path.display())))?;
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn unpack_tarball(tarball_path: &Path, dest_dir: &Path) -> AppResult<()> {
    let file = File::open(tarball_path).map_err(|err| {
        local_error(format!(
            "failed to open tarball {}: {err}",
            tarball_path.display()
        ))
    })?;
    let decoder =
        Decoder::new(file).map_err(|err| local_error(format!("tarball decode failed: {err}")))?;
    fs::create_dir_all(dest_dir).map_err(|err| {
        local_error(format!(
            "failed to create extraction directory {}: {err}",
            dest_dir.display()
        ))
    })?;
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries().map_err(|err| {
        local_error(format!(
            "failed to read tarball entries {}: {err}",
            tarball_path.display()
        ))
    })? {
        let mut entry = entry.map_err(|err| local_error(format!("tarball entry failed: {err}")))?;
        entry.unpack_in(dest_dir).map_err(|err| {
            local_error(format!(
                "failed to extract tarball entry into {}: {err}",
                dest_dir.display()
            ))
        })?;
    }
    Ok(())
}

pub fn resolve_extracted_root(dest_dir: &Path, expected_name: Option<&str>) -> AppResult<PathBuf> {
    if let Some(name) = expected_name {
        let candidate = dest_dir.join(name);
        if candidate.is_dir() {
            return Ok(candidate);
        }
    }

    let entries = fs::read_dir(dest_dir).map_err(|err| {
        local_error(format!(
            "failed to read extraction directory {}: {err}",
            dest_dir.display()
        ))
    })?;
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| local_error(format!("read_dir failed: {err}")))?;
        let path = entry.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }

    match dirs.len() {
        1 => Ok(dirs.remove(0)),
        0 => Err(invalid_argument("tarball did not contain a root directory")),
        _ => Err(invalid_argument(
            "tarball contained multiple root directories",
        )),
    }
}

fn invalid_argument(message: impl Into<String>) -> AppError {
    AppError::with_message(
        AppErrorKind::InvalidArgument,
        codes::INVALID_ARGUMENT,
        message,
    )
}

fn local_error(message: impl Into<String>) -> AppError {
    AppError::with_message(AppErrorKind::Internal, codes::LOCAL_ERROR, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn list_tar_entries(tarball_path: &Path) -> Vec<String> {
        let file = File::open(tarball_path).expect("open tarball");
        let decoder = Decoder::new(file).expect("decode tarball");
        let mut archive = tar::Archive::new(decoder);
        archive
            .entries()
            .expect("entries")
            .map(|entry| {
                let entry = entry.expect("entry");
                entry.path().expect("path").to_string_lossy().into_owned()
            })
            .collect()
    }

    fn contains_path(entries: &[String], candidate: &str) -> bool {
        entries
            .iter()
            .any(|entry| entry == candidate || entry == &format!("{candidate}/"))
    }

    #[test]
    fn create_tarball_rejects_non_dir() {
        let dir = tempdir().expect("temp dir");
        let file = dir.path().join("file.txt");
        fs::write(&file, "data").expect("write file");

        let tarball = dir.path().join("out.tar.zst");
        let err = create_tarball(&file, "project", &tarball, false).unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn create_tarball_respects_git_flag() {
        let dir = tempdir().expect("temp dir");
        fs::write(dir.path().join("a.txt"), "hello").expect("write file");
        fs::create_dir_all(dir.path().join("sub")).expect("sub dir");
        fs::write(dir.path().join("sub/b.txt"), "world").expect("write file");
        fs::create_dir_all(dir.path().join(".git")).expect("git dir");
        fs::write(dir.path().join(".git/config"), "config").expect("git file");

        let tarball_no_git = dir.path().join("out-no-git.tar.zst");
        create_tarball(dir.path(), "project", &tarball_no_git, false).expect("tarball");
        let entries = list_tar_entries(&tarball_no_git);
        assert!(contains_path(&entries, "project"));
        assert!(contains_path(&entries, "project/a.txt"));
        assert!(contains_path(&entries, "project/sub"));
        assert!(contains_path(&entries, "project/sub/b.txt"));
        assert!(!contains_path(&entries, "project/.git"));
        assert!(!contains_path(&entries, "project/.git/config"));

        let tarball_with_git = dir.path().join("out-with-git.tar.zst");
        create_tarball(dir.path(), "project", &tarball_with_git, true).expect("tarball");
        let entries = list_tar_entries(&tarball_with_git);
        assert!(contains_path(&entries, "project/.git"));
        assert!(contains_path(&entries, "project/.git/config"));
    }

    #[test]
    fn unpack_tarball_and_resolve_root() {
        let source = tempdir().expect("source");
        fs::write(source.path().join("a.txt"), "hello").expect("write file");
        fs::create_dir_all(source.path().join("sub")).expect("sub dir");
        fs::write(source.path().join("sub/b.txt"), "world").expect("write file");

        let tarball = source.path().join("archive.tar.zst");
        create_tarball(source.path(), "project", &tarball, false).expect("tarball");

        let dest = tempdir().expect("dest");
        unpack_tarball(&tarball, dest.path()).expect("unpack");
        let root = resolve_extracted_root(dest.path(), Some("project")).expect("root");
        assert_eq!(root, dest.path().join("project"));
        assert!(root.join("a.txt").is_file());
        assert!(root.join("sub/b.txt").is_file());
    }

    #[test]
    fn resolve_extracted_root_falls_back_to_single_dir() {
        let dest = tempdir().expect("dest");
        let root = dest.path().join("project");
        fs::create_dir_all(&root).expect("root dir");
        let resolved = resolve_extracted_root(dest.path(), Some("missing")).expect("resolve");
        assert_eq!(resolved, root);
    }

    #[test]
    fn resolve_extracted_root_errors_on_missing_or_multiple_dirs() {
        let empty = tempdir().expect("empty dir");
        fs::write(empty.path().join("file.txt"), "data").expect("write file");
        let err = resolve_extracted_root(empty.path(), None).unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);

        let multiple = tempdir().expect("multiple dirs");
        fs::create_dir_all(multiple.path().join("one")).expect("dir one");
        fs::create_dir_all(multiple.path().join("two")).expect("dir two");
        let err = resolve_extracted_root(multiple.path(), None).unwrap_err();
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn create_tarball_handles_complex_nested_structure() {
        let root = tempdir().expect("root");
        fs::create_dir_all(root.path().join("alpha/beta/gamma")).expect("dirs");
        fs::create_dir_all(root.path().join("alpha/empty")).expect("empty dir");
        fs::create_dir_all(root.path().join("zeta")).expect("zeta dir");
        fs::write(root.path().join("alpha/beta/gamma/data.txt"), "payload").expect("write");
        fs::write(root.path().join("alpha/beta/config.json"), "{\"ok\":true}").expect("write");
        fs::write(root.path().join("alpha/notes.txt"), "notes").expect("write");
        fs::write(root.path().join("zeta/readme.md"), "# docs").expect("write");
        fs::write(root.path().join(".env"), "KEY=value").expect("write");

        let tarball = root.path().join("complex.tar.zst");
        create_tarball(root.path(), "project", &tarball, false).expect("tarball");

        let entries = list_tar_entries(&tarball);
        let expected = [
            "project",
            "project/.env",
            "project/alpha",
            "project/alpha/notes.txt",
            "project/alpha/empty",
            "project/alpha/beta",
            "project/alpha/beta/config.json",
            "project/alpha/beta/gamma",
            "project/alpha/beta/gamma/data.txt",
            "project/zeta",
            "project/zeta/readme.md",
        ];
        for item in expected {
            assert!(contains_path(&entries, item), "missing entry: {item}");
        }
    }

    #[test]
    fn unpack_tarball_restores_complex_nested_structure() {
        let root = tempdir().expect("root");
        fs::create_dir_all(root.path().join("alpha/beta/gamma")).expect("dirs");
        fs::create_dir_all(root.path().join("alpha/empty")).expect("empty dir");
        fs::create_dir_all(root.path().join("zeta")).expect("zeta dir");
        fs::write(root.path().join("alpha/beta/gamma/data.txt"), "payload").expect("write");
        fs::write(root.path().join("alpha/beta/config.json"), "{\"ok\":true}").expect("write");
        fs::write(root.path().join("alpha/notes.txt"), "notes").expect("write");
        fs::write(root.path().join("zeta/readme.md"), "# docs").expect("write");
        fs::write(root.path().join(".env"), "KEY=value").expect("write");

        let tarball = root.path().join("complex.tar.zst");
        create_tarball(root.path(), "project", &tarball, false).expect("tarball");

        let dest = tempdir().expect("dest");
        unpack_tarball(&tarball, dest.path()).expect("unpack");
        let extracted = resolve_extracted_root(dest.path(), Some("project")).expect("root");

        let data = fs::read_to_string(extracted.join("alpha/beta/gamma/data.txt")).expect("read");
        assert_eq!(data, "payload");
        let config = fs::read_to_string(extracted.join("alpha/beta/config.json")).expect("read");
        assert_eq!(config, "{\"ok\":true}");
        let notes = fs::read_to_string(extracted.join("alpha/notes.txt")).expect("read");
        assert_eq!(notes, "notes");
        let readme = fs::read_to_string(extracted.join("zeta/readme.md")).expect("read");
        assert_eq!(readme, "# docs");
        let env = fs::read_to_string(extracted.join(".env")).expect("read");
        assert_eq!(env, "KEY=value");
        assert!(extracted.join("alpha/empty").is_dir());
    }

    #[test]
    fn blake3_file_hash_matches_reference_digest() {
        let root = tempdir().expect("root");
        let file = root.path().join("sample.txt");
        fs::write(&file, "orbit").expect("write");

        let hash = blake3_file_hash(&file).expect("hash");
        let expected = blake3::hash(b"orbit").to_hex().to_string();
        assert_eq!(hash, expected);
    }
}
