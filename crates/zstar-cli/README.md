# Zstar CLI

Install with `cargo install zstar-cli` or build with `cargo build -p zstar-cli`.
The executable is named `zstar` (`target/debug/zstar` for a local build).
All commands use the entry-oriented manifest described in [the specification](../../spec/v1.md).

```sh
zstar pack input-directory archive.zstar
zstar inspect archive.zstar
zstar extract archive.zstar destination-directory
zstar extract thin.zstar destination-directory --objects local-objects
zstar pack input-directory s3://bucket/archive.zstar
zstar inspect s3://bucket/archive.zstar
zstar extract s3://bucket/archive.zstar destination-directory
```

`pack` streams regular files, directories, and symlinks in UTF-8 path order.
It normalizes modes to files 0644 (0755 when executable), directories 0755,
symlinks 0777, with UID/GID and mtime zero and special bits cleared.
`--preserve-metadata` retains source mode, UID/GID, and nanosecond mtime.
Exact UTF-8 paths and link targets are retained. Unsupported
entry types and non-UTF-8 names fail. Capture uses directory handles without
following links; the input root must be a real directory. Metadata is sorted
through a bounded spool; file payloads use bounded frames. Output must be
outside the input tree (symlinked outputs are resolved) and must not exist
unless `--force` is given. Output is staged and published only after successful
completion, with the usual new-file permissions (0666 filtered by umask).
`--compression-level` accepts -131072 to 22 and `--workers` 1 to 16.

`inspect` authenticates and validates metadata without reading data frames.
Metadata-only inspection does not establish validity of unread physical content.

`extract` uses captured destination directory handles, exclusive file creation,
and no-follow opens. Descendant entries must not already exist. The destination
root may be an existing real directory, but cannot be a symlink. Files are
written before links; directory metadata is applied last. Absolute, dangling,
cyclic, and `..` link targets are preserved verbatim and never accessed.
Restoration applies modes and mtime, strips setuid/setgid unless
`--allow-special-bits` is supplied, and does not apply ownership. Anything
recorded but not applied (ownership, masked mode bits, symlink modes that the
platform cannot set) is summarized on stderr as `note:` lines; symlink modes
never abort extraction. A destination that does not exist is built in a
temporary sibling and renamed on success, so failure leaves nothing behind. For
an existing destination (including `.` and `/`), entries created by the failed
run are removed and pre-existing content is untouched. Errors name the archive
path. Linux/macOS Unix filesystems are supported.

Thin archives use `--objects DIR`, reading independent bounded Zstandard
objects from `DIR/<lowercase SHA-256>.zst`. Actual object lengths are bounded
independently of encoded-size hints. Objects are decoded and their raw SHA-256
is checked before selecting extents. `--skip-external-hash` on `extract`
skips only external-object hashing. Metadata authentication,
codec bounds, archive data checksums, and physical structure checks remain
mandatory. Inline content has no SHA-256 in this draft format.

Resource controls are local policy, not wire-format entry-count limits:

- `--metadata-memory-bytes`: 8 MiB before temporary-file spill.
- `--max-metadata-bytes`: 4 GiB aggregate encoded/decoded metadata limit.
- Pack: `--frame-size` (1 MiB), `--compression-level` (3),
  `--workers` (up to two by default), and
  `--metadata-block-size` (64 KiB).
- Extract: `--max-directory-handles` (64, including the destination root).
  A limit, not a tree-size cap: least-recently-used handles are evicted and
  reopened without following links; the minimum is 2.

Pack outputs and inspect/extract inputs accept `s3://bucket/key`. Credentials
come from the standard AWS SDK credential chain, including shared profiles
(`AWS_PROFILE`), environment credentials, web identity, and instance roles.
For example:

```sh
AWS_PROFILE=production zstar inspect s3://bucket/archive.zstar
```

For an S3-compatible service, put the endpoint and addressing style in the
selected AWS shared config profile. `AWS_ENDPOINT_URL_S3` and
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST` can override those profile settings when
needed. Remote archives are staged locally, and uploads/downloads currently
buffer the complete archive in memory for the transfer.
