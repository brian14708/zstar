# Reader budgets and shared caches

Reader options apply to `Archive` and all `AsyncArchive` constructors, as well
as the low-level readers in `zstar.format`. Sources, custom spill stores, and shared caches
remain caller-owned. Keep encoded sources immutable for the reader's lifetime.

Metadata spools default to 8 MiB in memory and 4 GiB encoded/decoded limits;
`memory_bytes`, `max_encoded_bytes`, and `max_decoded_bytes` configure them.
`cache_bytes` defaults to 16 MiB of reader-local decoded-content reuse. Zero
disables that cache. Parsed entries use O(N) memory beyond the spool threshold.
Returned reads are limited by `max_read_bytes` (64 MiB). Active decoded frames and returned
Python bytes are additional to charged cache capacity.

`max_spool_bytes` limits retained logical bytes across one reader or writer's
spools, including in-memory spools (4 GiB by default). It excludes filesystem
block overhead, transient migration copies, decoded caches, and caller-retained
buffers. Reservations precede store I/O; failed partial writes retain their
charge until the spool is released. Quota exhaustion is reported before the
next store operation. `create_spill_store` optionally supplies a
fresh empty binary object with `read`, `write`, `seek`, and `flush` methods.
Spool options apply to `Archive`, `AsyncArchive`, `ArchiveWriter`, and the
low-level readers and writer in `zstar.format`. Custom stores are caller-owned
and must be used exclusively until the reader or writer releases them. The SDK does not close them:

```python
from contextlib import ExitStack
from tempfile import TemporaryFile
from zstar import Archive, pack_file

encoded = pack_file("hello.txt", b"hello")
with ExitStack() as stores:

    def create_store():
        return stores.enter_context(TemporaryFile())

    with Archive(encoded, memory_bytes=1 << 20, create_spill_store=create_store) as archive:
        result = archive.read("hello.txt")
```

To share decoded chunks across readers, use a caller-owned bounded
`DecodedMemoryCache` and provide the exact immutable encoded archive identity:

```python
from hashlib import sha256
from zstar import Archive, DecodedMemoryCache, pack_file

encoded = pack_file("hello.txt", b"hello")
shared = DecodedMemoryCache(32 << 20)
with Archive(
    encoded,
    shared_cache=shared,
    shared_cache_identity=sha256(encoded).digest(),
) as archive:
    assert archive.read("hello.txt") == b"hello"
```

Metadata SHA-256 alone cannot identify inline encoded data. Cache keys include
the caller's source identity, authenticated metadata digest and encoded locator.
Verified external objects use their content digest; unchecked external reads
bypass the shared cache. The cache object remains caller-owned, and returned
`bytes` stay valid after `clear()`.
A custom cache can implement synchronous `get(key)` and `put(token)` methods.
`get` returns `None` for a miss or a previously received `DecodedChunk` token.
Tokens have a stable string `key`, immutable copied `data: bytes`, and retained
allocation `weight`; callers cannot construct or modify them. Bound the cache's
retained storage, support concurrent callbacks, and never re-enter its readers.
The reader validates token provenance and keys; wrong tokens and callback
exceptions fall back to normal reads. Custom callbacks run on bounded workers
for indexed/async readers. Cancellation does not free a worker permit until
its callback finishes. Callbacks are synchronous and must eventually return;
closing a reader does not close the cache. All archive constructors accept the
same `shared_cache` and `shared_cache_identity` options.

# Free-threading (PEP 703)

**Status.** The native module declares `gil_used = false`, every native class is
`Sync` (frozen or internally locked), and the test-suite includes concurrency
stress tests (`pytest -m free_threading`, also part of the normal run). A
free-threaded wheel has not been published; see the status note at the end of
this section for what has been exercised. On a GIL build nothing changes.

## Thread-safety guarantees

| Object | Sharing between threads |
| --- | --- |
| `Archive`, `IndexedArchive` | Safe. Reads, metadata, `verify`, and `close` may race; a read racing `close` finishes or raises "closed". |
| `AsyncArchive` | Safe from any thread and any event loop; futures resolve on the loop that called. |
| `ArchiveFile` (sync stream) | Safe to share. One per-stream lock covers `read`, `readinto`, `seek`, `tell`, and `close` and is held across the native read, so concurrent readers get disjoint slices. Separate streams never contend. |
| `AsyncArchiveFile` | **One event loop only.** Its `asyncio.Lock` serializes tasks of a single loop and gives no cross-thread exclusion. From other threads call `asyncio.run_coroutine_threadsafe(stream.read(n), loop)`. |
| Iterators (`Archive.getmembers()` generator) | Generators are single-consumer, as in Python. The native cursors behind them (`iter_entries()`) serialize concurrent `next()` calls instead of raising "already borrowed". |
| `ManifestArchive`, `ManifestEntries`, `EntryArchiveWriter`, `ArchiveWriter` | Safe but serialized: operations run one at a time; waiting threads release the GIL/attached state. `closed` and `entry_count` never wait. |
| `DecodedMemoryCache`, `DecodedChunk` | Safe (internally locked / immutable). |
| `ZstarFileSystem` | Safe. A lock guards the archive cache and per-archive pin counts; the opener runs outside it and concurrent loads of one archive share a single open. |

**Callbacks are not re-entrant.** A source, resolver, sink, body, or spill-store
callback running inside a `ManifestArchive` or `EntryArchiveWriter` operation
must not call back into that same object: the call raises `ZstarError`
(category `unsupported-operation`) rather than deadlocking. Other threads may
wait, but they wait detached from Python, so the callback can always make
progress. No native lock is held while Python code runs or while a thread waits
for another thread that needs Python.

**Failure semantics.** A writer that fails (including a re-entrant or failing
callback outside the rejection above) is poisoned for every thread: later calls
raise `ValueError("writer failed")`. Panics in native code never poison state
locks into panicking every other thread.

**Async methods.** `AsyncArchive.getmember`, `extractfile`, and the other
metadata calls are coroutines and never block the event loop on the native
runtime, so a provider scheduled on the same loop cannot deadlock them.

## Building a free-threaded wheel (abi3 caveat)

The default build is `abi3-py311` (one `cp311-abi3` wheel for all GIL builds).
CPython 3.13t/3.14t have no stable ABI before 3.15, so a free-threaded wheel
must be version-specific. The `abi3` Cargo feature (on by default) enables
`pyo3/abi3-py311`; turn it off for free-threaded interpreters:

```console
maturin build --release --no-default-features -i python3.14t
# or, with uv, against a free-threaded interpreter:
uv build --python 3.14t --config-setting build-args="--no-default-features"
```

What PyO3 0.30 does if abi3 is left on: `pyo3-build-config` detects
`Py_GIL_DISABLED`, warns "Targeting an abi3 build but build_flags contains
Py_GIL_DISABLED, falling back to a version-specific free-threaded build", and
links against the free-threaded interpreter. The extension then compiles
correctly, but maturin still derives the wheel tag from the feature and
publishes it as `cp311-abi3`, which free-threaded Python refuses to load. Always
pass `--no-default-features` for `cp313t`/`cp314t`. With the feature off, GIL
interpreters also get a version-specific wheel.

Run the suite under the free-threaded interpreter with `PYTHON_GIL=0`; the
`test_native_module_does_not_re_enable_the_gil` test (skipped elsewhere) fails
if importing the extension turns the GIL back on.
