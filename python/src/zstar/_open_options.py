"""Separate native archive policy from nested transport settings on open."""

from typing import cast

from . import _native
from .types import ReaderOptions, SinkOptions, SourceOptions, WriterOptions


def reader_options(
    options: ReaderOptions | None,
) -> tuple[_native.ReaderConfig, SourceOptions | None]:
    if options is not None and not isinstance(options, dict):
        raise TypeError("options must be a dictionary")
    policy = dict(options or {})
    source = policy.pop("source", None)
    return _native.ReaderConfig(cast(ReaderOptions, policy)), cast(SourceOptions | None, source)


def writer_options(
    options: WriterOptions | None,
) -> tuple[_native.WriterConfig, SinkOptions | None]:
    if options is not None and not isinstance(options, dict):
        raise TypeError("options must be a dictionary")
    policy = dict(options or {})
    sink = policy.pop("sink", None)
    return _native.WriterConfig(cast(WriterOptions, policy)), cast(SinkOptions | None, sink)
