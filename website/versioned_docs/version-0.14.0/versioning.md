---
title: Documentation Versions
description: How Orbit docs are versioned by release and how to view the right CLI reference for your installed version.
---

Orbit docs are release-versioned.

- `next` contains unreleased docs from `main`.
- Numbered versions (for example `0.14.0`) are frozen snapshots created at release time.

## For users

Use the version dropdown in the top navigation to select the docs version that matches your installed Orbit version.

You can check your installed version with:

```bash
orbit --version
```

## For maintainers

Release automation snapshots docs for each release and includes generated CLI reference pages for that release.

Manual command (if needed):

```bash
./scripts/version_docs.sh <x.y.z>
```

This command:

1. Regenerates CLI reference from `orbit --help`.
2. Creates a Docusaurus docs snapshot for `<x.y.z>`.
