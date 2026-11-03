---
title: Getting started
description: Generate your first archive, read it, and try S3.
---

Start by creating a local archive: a compressed file that holds other files.
Choose your language below to write `example.zstar`, read a file from it, and
check it for errors. Each example creates its own sample text, so you do not
need any input files.

| SDK | Start here | Then try |
| --- | --- | --- |
| Python | [Create your first archive](/zstar/tutorials/python/first-archive/) | [Read from S3](/zstar/tutorials/python/s3/) |
| Rust | [Create your first archive](/zstar/tutorials/rust/first-archive/) | [Read from S3](/zstar/tutorials/rust/s3/) |
| Go | [Create your first archive](/zstar/tutorials/go/first-archive/) | [Read from S3](/zstar/tutorials/go/s3/) |

The S3 tutorials generate and upload the same archive, read it through an S3
adapter, and delete it afterward. You supply a test bucket and credentials.

For command-line use, see the [CLI guide](https://github.com/brian14708/zstar/tree/main/crates/zstar-cli).
