import asyncio
import io
import json
from pathlib import Path

import pytest
import zstar

ROOT = Path(__file__).resolve().parents[2] / "testdata" / "packed-objects"


@pytest.mark.parametrize("dataset", ["", "go", "python"])
def test_shared_packed_fixtures(dataset):
    fixture = ROOT / dataset
    catalog = zstar.PackedCatalog((fixture / "catalog.cbor").read_bytes())
    assert catalog.encode() == (fixture / "catalog.cbor").read_bytes()
    resolver = zstar.PackedObjectResolver(zstar.PackStore.local(fixture), catalog)
    with zstar.Archive.open(fixture / "thin.zstar", resolver=resolver) as archive:
        assert archive.read("payload") == (fixture / "payload.bin").read_bytes()
        assert archive.read("payload", 4, 7) == b"tsecond"
    generation = zstar.PackedGeneration((fixture / "generation.cbor").read_bytes())
    assert generation.encode() == (fixture / "generation.cbor").read_bytes()


@pytest.mark.parametrize(
    "case",
    json.loads((ROOT / "manifest.json").read_text())["negative_catalogs"],
    ids=lambda case: case["file"],
)
def test_negative_packed_catalog_fixtures(case):
    with pytest.raises(zstar.ZstarError) as error:
        zstar.PackedCatalog((ROOT / case["file"]).read_bytes())
    assert error.value.category == case["category"]


def test_native_packed_workflow(tmp_path):
    store = zstar.PackStore.local(tmp_path)
    packs = zstar.PackWriter(store, target_bytes=1024, memory_bytes=64)
    data = bytes(range(256)) * 16384
    strategy = zstar.Chunking("fastcdc", target_bytes=256 << 10)
    body = packs.externalize(io.BytesIO(data), len(data), chunking=strategy)
    info = zstar.TarInfo("payload")
    info.size = len(data)
    thin = io.BytesIO()
    with zstar.ArchiveWriter(thin) as writer:
        writer.add_external(info, body)
    catalog = packs.finish()
    assert catalog.chunk_count < body.chunk_count
    assert packs.completed_packs()
    resolver = zstar.PackedObjectResolver(store, catalog)
    with zstar.Archive(thin.getvalue(), resolver=resolver) as archive:
        assert archive.read("payload") == data
    key, generation = zstar.publish_packed_generation(
        store, io.BytesIO(thin.getvalue()), catalog, chunking=strategy
    )
    assert (tmp_path / key).read_bytes() == generation.encode()
    with pytest.raises(zstar.ZstarError):
        packs.externalize(io.BytesIO(data), len(data))


def test_async_native_packed_workflow(tmp_path):
    async def run():
        store = zstar.PackStore.local(tmp_path)
        packs = zstar.PackWriter(store)
        body = await packs.externalize_async(io.BytesIO(b"one two three"), 13)
        thin = io.BytesIO()
        info = zstar.TarInfo("file")
        info.size = body.size
        with zstar.ArchiveWriter(thin) as writer:
            writer.add_external(info, body)
        catalog = await packs.finish_async()
        resolver = zstar.PackedObjectResolver(store, catalog)
        async with await zstar.AsyncArchive.from_bytes(
            thin.getvalue(), resolver=resolver
        ) as archive:
            assert await archive.read("file") == b"one two three"
            assert await archive.read_many([("file", 0, 3), ("file", 4, 3)]) == [b"one", b"two"]
        key, generation = await zstar.publish_packed_generation_async(
            store, io.BytesIO(thin.getvalue()), catalog
        )
        assert (tmp_path / key).read_bytes() == generation.encode()

    asyncio.run(run())


def test_pack_writer_reentry_and_abort(tmp_path):
    with zstar.PackWriter(zstar.PackStore.local(tmp_path)) as packs:

        class Body(io.BytesIO):
            def read(self, size=-1):
                with pytest.raises(zstar.ZstarError, match="busy or reentrant"):
                    packs.finish()
                return super().read(size)

        body = packs.externalize(Body(b"data"), 4)
        assert body.size == 4
    with pytest.raises(zstar.ZstarError, match="failed or closed"):
        packs.finish()


def test_packed_fsspec_sync_and_async():
    store = zstar.PackStore.local(ROOT)
    catalog = zstar.PackedCatalog((ROOT / "catalog.cbor").read_bytes())
    resolver = zstar.PackedObjectResolver(store, catalog)
    fs = zstar.ZstarFileSystem(fo=ROOT / "thin.zstar", resolver=resolver)
    try:
        assert fs.cat_file("payload") == (ROOT / "payload.bin").read_bytes()
    finally:
        fs.close()

    async def run():
        fs = zstar.ZstarFileSystem(fo=ROOT / "thin.zstar", resolver=resolver, asynchronous=True)
        try:
            assert await fs._cat_file("payload") == (ROOT / "payload.bin").read_bytes()
        finally:
            await fs.aclose()

    asyncio.run(run())
