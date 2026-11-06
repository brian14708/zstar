---
title: Zstar
description: A core specification for indexed tar.zst archives, with optional extensions.
template: splash
hero:
  tagline: Compressed archives. Direct access.
  actions:
    - text: Read the format
      link: reference/spec/
      icon: right-arrow
    - text: Use an SDK
      link: getting-started/
      variant: minimal
---

Zstar specifies an indexed `.tar.zst` archive format. The
[core specification](reference/spec/core/) keeps all file content in the archive
and lets readers fetch individual files or byte ranges. Optional extensions
build on that core, including
[external content](reference/spec/external-content/) for references to objects
identified by SHA-256. A separate
[catalog proposal](reference/spec/catalog/) organizes archives into collections.

## Choose an SDK

Use [Python](sdks/python/) with sync, async, or fsspec APIs; [Rust](sdks/rust/)
with async readers and writers; or [Go](sdks/go/) with Go streams. All three
support local files, HTTP ranges, and S3.
The SDKs implement the core format and the optional external-content extension.

## Explore the reference

See the [Python API reference](/zstar/api/python/) for methods and options, or the
[draft specifications](reference/spec/) for the core format and optional additions.
