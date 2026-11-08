"""Metadata page limits and memory-only working-buffer budgets."""

import asyncio
import io

import pytest
import zstar
from _support import maybe


@pytest.mark.parametrize("asynchronous", [False, True])
def test_reader_metadata_limits(asynchronous, inline_data):
    async def run():
        with pytest.raises(zstar.ZstarError) as caught:
            await maybe(
                zstar.open(
                    inline_data,
                    options={"metadata": {"max_page_bytes": 1}},
                    async_=asynchronous,
                )
            )
        assert caught.value.category == "resource-limit"

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_writer_buffer_limit(asynchronous):
    options = {"metadata": {"max_buffer_bytes": 1}}

    async def run():
        writer = await maybe(
            zstar.open(io.BytesIO(), options=options, mode="w", async_=asynchronous)
        )
        with pytest.raises(zstar.ZstarError, match="metadata buffer size") as caught:
            await maybe(writer.writestr("file", b"data"))
        assert caught.value.category == "resource-limit"
        await maybe(writer.abort())

    asyncio.run(run())
