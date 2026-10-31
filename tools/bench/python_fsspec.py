"""Compare the old core API and fsspec facade using one shared local corpus.

Run with each installed wheel: python tools/bench/python_fsspec.py CORPUS --prepare
then omit --prepare for subsequent versions. Timings include archive close and
member verification. No disk-cache eviction or network is simulated.
"""

import argparse
import asyncio
import io
import json
import random
import statistics
import time
from pathlib import Path
from tarfile import TarInfo

import fsspec
import zstar
from zstar._fsspec import ZstarFileSystem
from zstar._writer import ArchiveWriter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("corpus", type=Path)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--samples", type=int, default=15)
    args = parser.parse_args()
    args.corpus.mkdir(parents=True, exist_ok=True)
    modern = hasattr(zstar.Archive, "from_file")
    reader = zstar.Archive.from_file if modern else zstar.Archive.open
    writer = ArchiveWriter.from_file if modern else ArchiveWriter.open
    large = args.corpus / "large.zstar"
    small = args.corpus / "small.zstar"
    source = args.corpus / "payload.bin"
    if args.prepare:
        payload = random.Random(42).randbytes(32 << 20)
        source.write_bytes(payload)
        with writer(large) as archive:
            info = TarInfo("data")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
        with writer(small) as archive:
            for i in range(128):
                info = TarInfo(f"file-{i:03}")
                info.size = 8192
                archive.addfile(info, io.BytesIO(payload[i * 8192 : (i + 1) * 8192]))
    body = source.read_bytes()
    output = args.corpus / "output.zstar"

    def native_large():
        with reader(large) as archive:
            assert len(archive.read("data")) == len(body)

    def fs_large():
        fs = ZstarFileSystem(fo=large)
        try:
            assert len(fs.cat_file("data")) == len(body)
        finally:
            fs.close()

    def native_small():
        with reader(small) as archive:
            for i in range(128):
                assert len(archive.read(f"file-{i:03}")) == 8192

    def fs_small():
        fs = ZstarFileSystem(fo=small)
        try:
            for i in range(128):
                assert len(fs.cat_file(f"file-{i:03}")) == 8192
        finally:
            fs.close()

    async def fs_async_small_impl():
        async with ZstarFileSystem(fo=small, asynchronous=True) as fs:
            for i in range(128):
                assert len(await fs._cat_file(f"file-{i:03}")) == 8192

    def fs_async_small():
        asyncio.run(fs_async_small_impl())

    def core_write():
        with writer(output) as archive:
            info = TarInfo("data")
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))

    def public_write():
        if modern:
            with ZstarFileSystem(fo=output, mode="w") as fs:
                fs.pipe_file("data", body)
        else:
            core_write()

    def core_write_small():
        with writer(output) as archive:
            for i in range(128):
                info = TarInfo(f"file-{i:03}")
                info.size = 8192
                archive.addfile(info, io.BytesIO(body[:8192]))

    def public_write_small():
        if modern:
            with ZstarFileSystem(fo=output, mode="w") as fs:
                for i in range(128):
                    fs.pipe_file(f"file-{i:03}", body[:8192])
        else:
            core_write_small()

    results = {}
    for operation in (
        native_large,
        fs_large,
        native_small,
        fs_small,
        fs_async_small,
        core_write,
        public_write,
        core_write_small,
        public_write_small,
    ):
        operation()  # warm imports, native runtime, and OS page cache
        samples = []
        for _ in range(args.samples):
            start = time.perf_counter_ns()
            operation()
            samples.append((time.perf_counter_ns() - start) / 1e6)
        results[operation.__name__] = {
            "median_ms": statistics.median(samples),
            "samples_ms": samples,
        }
    print(json.dumps({"modern": modern, "fsspec": fsspec.__version__, "results": results}))


if __name__ == "__main__":
    main()
