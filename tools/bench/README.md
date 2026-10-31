# Python fsspec benchmark

Compared commit `66190e5` with the working implementation on an AMD Ryzen 7 7840HS,
Linux x86_64, Python 3.14.8, fsspec 2026.9.0, release-mode native extensions.
Each result is the median of 15 runs after one warm-up. Both versions read the
same archives. Inputs contain seeded pseudorandom bytes; filesystem caches are
warm. Timings include open and close, but exclude input generation. No fsync,
cold-cache, network, or S3 latency is measured. Differences of a few percent
should be treated as noise on this shared host.

| Workload | Old (ms) | Current (ms) | Change |
| --- | ---: | ---: | ---: |
| fsspec read 32 MiB member | 55.93 | 54.86 | -1.9% |
| fsspec sync read 128 × 8 KiB members | 27.58 | 28.75 | +4.2% |
| fsspec async read 128 × 8 KiB members | 23.15 | 24.11 | +4.1% |
| Write 32 MiB | 24.45 | 20.04 | -18.0% |
| Write 128 × 8 KiB members | 1.06 | 13.66 | 12.9× |

The old implementation has no writable fsspec interface: write rows compare its
raw `ArchiveWriter.addfile()` with the new `fs.pipe_file()` facade. Therefore these
rows measure the cost of adopting the facade, not an equivalent implementation
regression. Both versions retain a raw writer. Before eliminating unnecessary
staging, the new 32 MiB write took 32.78 ms; known-size bodies now stream directly
to the encoder. The 128-member case retains one Python/async/native dispatch per
insertion; raw `addfile()` is preferable when that overhead dominates.

Reproduce with separate installed environments, using the same Python and fsspec
versions. Build the original commit in a separate Cargo target directory; do not
share wheel output directories between builds. Use the same benchmark script for
both environments:

```sh
python -I tools/bench/python_fsspec.py /tmp/zstar-corpus --prepare > current.json
# In the environment containing the old wheel (do not prepare again):
python -I tools/bench/python_fsspec.py /tmp/zstar-corpus > baseline.json
```

The script also records direct reader/writer timings and all individual samples.
These measurements establish local API overhead; remote transport performance
needs a separate benchmark with representative object sizes, range counts,
connection reuse, and network latency.

## Current fsspec versus raw Python API

| Workload | Raw API (ms) | fsspec sync (ms) | fsspec/raw |
| --- | ---: | ---: | ---: |
| Read 32 MiB | 34.83 | 54.86 | 1.57× |
| Read 128 × 8 KiB | 0.93 | 28.75 | 31.0× |
| Write 32 MiB | 24.85 | 20.04 | 0.81× |
| Write 128 × 8 KiB | 1.06 | 13.66 | 12.9× |

Both columns invoke the Rust archive engine through Python. This does not measure
a standalone Rust client. The raw local reader uses native positional I/O; the
fsspec reader uses backend callbacks and the sync-to-async facade. Writing differs
in execution context and backend commit strategy, so the single-large-file result
should not be generalized to all writer workloads. Native async fsspec reading of
128 small members took 24.11 ms in this run.
