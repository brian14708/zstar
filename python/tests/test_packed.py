import asyncio
import io
import json
from pathlib import Path
from tarfile import TarInfo

import pytest
import zstar
from zstar import ArchiveWriter, _native

ROOT = Path(__file__).resolve().parents[2] / "testdata" / "packed-objects"


@pytest.mark.parametrize("dataset", ["", "go", "python"])
def test_shared_packed_fixtures(dataset):
    fixture = ROOT / dataset
    catalog = _native.PackedCatalog((fixture / "catalog.cbor").read_bytes())
    assert catalog.encode() == (fixture / "catalog.cbor").read_bytes()
    resolver = _native.PackedObjectResolver(_native.PackStore.local(fixture), catalog)
    with zstar.Archive.from_file(fixture / "thin.zstar", resolver=resolver) as archive:
        assert archive.read("payload") == (fixture / "payload.bin").read_bytes()
        assert archive.read("payload", 4, 7) == b"tsecond"
    generation = _native.PackedGeneration((fixture / "generation.cbor").read_bytes())
    assert generation.encode() == (fixture / "generation.cbor").read_bytes()


@pytest.mark.parametrize(
    "case",
    json.loads((ROOT / "manifest.json").read_text())["negative_catalogs"],
    ids=lambda case: case["file"],
)
def test_negative_packed_catalog_fixtures(case):
    with pytest.raises(zstar.ZstarError) as error:
        _native.PackedCatalog((ROOT / case["file"]).read_bytes())
    assert error.value.category == case["category"]


def test_native_packed_workflow(tmp_path):
    store = _native.PackStore.local(tmp_path)
    packs = _native.PackWriter(store, target_bytes=1024, memory_bytes=64)
    data = bytes(range(256)) * 16384
    strategy = _native.Chunking("fastcdc", target_bytes=256 << 10)
    body = packs.externalize(io.BytesIO(data), len(data), chunking=strategy)
    info = TarInfo("payload")
    info.size = len(data)
    thin = io.BytesIO()
    with ArchiveWriter(thin) as writer:
        writer.add_external(info, body)
    catalog = packs.finish()
    assert catalog.chunk_count < body.chunk_count
    assert packs.completed_packs()
    resolver = _native.PackedObjectResolver(store, catalog)
    with zstar.Archive(thin.getvalue(), resolver=resolver) as archive:
        assert archive.read("payload") == data
    key, generation = _native.publish_packed_generation(
        store, io.BytesIO(thin.getvalue()), catalog, chunking=strategy
    )
    assert (tmp_path / key).read_bytes() == generation.encode()
    with pytest.raises(zstar.ZstarError):
        packs.externalize(io.BytesIO(data), len(data))


def test_async_native_packed_workflow(tmp_path):
    async def run():
        store = _native.PackStore.local(tmp_path)
        packs = _native.PackWriter(store)
        body = await packs.externalize_async(io.BytesIO(b"one two three"), 13)
        thin = io.BytesIO()
        info = TarInfo("file")
        info.size = body.size
        with ArchiveWriter(thin) as writer:
            writer.add_external(info, body)
        catalog = await packs.finish_async()
        resolver = _native.PackedObjectResolver(store, catalog)
        async with await zstar.AsyncArchive.from_bytes(
            thin.getvalue(), resolver=resolver
        ) as archive:
            assert await archive.read("file") == b"one two three"
            assert await archive.read_many([("file", 0, 3), ("file", 4, 3)]) == [b"one", b"two"]
        key, generation = await _native.publish_packed_generation_async(
            store, io.BytesIO(thin.getvalue()), catalog
        )
        assert (tmp_path / key).read_bytes() == generation.encode()

    asyncio.run(run())


def test_pack_writer_reentry_and_abort(tmp_path):
    with _native.PackWriter(_native.PackStore.local(tmp_path)) as packs:

        class Body(io.BytesIO):
            def read(self, size=-1):
                with pytest.raises(zstar.ZstarError, match="busy or reentrant"):
                    packs.finish()
                return super().read(size)

        body = packs.externalize(Body(b"data"), 4)
        assert body.size == 4
    with pytest.raises(zstar.ZstarError, match="failed or closed"):
        packs.finish()
