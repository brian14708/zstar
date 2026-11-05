---
title: Zstar
description: Indexed tar.zst archives with random-access readers and writers.
template: splash
hero:
  tagline: Compressed archives. Direct access.
  actions:
    - text: Get started
      link: getting-started/
      icon: right-arrow
    - text: Read the format
      link: reference/spec/
      variant: minimal
---

Zstar stores files in compressed Zstandard frames. Its index lets readers fetch
only the frames they need. Files can stay in the archive or use external objects
identified by SHA-256.

## Choose an SDK

Use [Python](sdks/python/) with sync, async, or fsspec APIs; [Rust](sdks/rust/)
with async readers and writers; or [Go](sdks/go/) with Go streams. All three
support local files, HTTP ranges, and S3.

## Explore the reference

See the [Python API reference](/zstar/api/python/) for methods and options, or the
[draft specifications](reference/spec/) for the [core format](reference/spec/core/),
[external-content extension](reference/spec/external-content/), and informative
[CAS storage appendix](reference/spec/external-content/#appendix-cas-storage).

The [catalog proposal](reference/spec/catalog/) describes named JSON collections
of CAS-stored archive layers; catalog operations remain unimplemented.
