"""Explicit conversion between Zstar metadata and tarfile.TarInfo."""

import warnings
from tarfile import DIRTYPE, REGTYPE, SYMTYPE, TarInfo

from ._info import _nanoseconds
from ._metadata import Entry, EntryKind, Header
from .types import ExtensionValue


def from_tarinfo(info: TarInfo, *, extensions: dict[str, ExtensionValue] | None = None) -> Header:
    """Copy supported tar metadata, preferring the exact PAX timestamp."""
    if not isinstance(info, TarInfo):
        raise TypeError("info must be TarInfo")
    metadata = {
        "mode": info.mode,
        "uid": info.uid,
        "gid": info.gid,
        "mtime_ns": _nanoseconds(info.pax_headers.get("mtime", info.mtime)),
        "extensions": extensions,
    }
    if info.isfile():
        return Header.file(info.name, info.size, **metadata)
    if info.isdir():
        return Header.directory(info.name, **metadata)
    if info.issym():
        return Header.symlink(info.name, info.linkname, **metadata)
    raise ValueError("only regular files, directories, and symlinks are supported")


def to_tarinfo(entry: Entry) -> TarInfo:
    """Project an entry, warning about extensions omitted by tar metadata.

    PAX mtime preserves nanoseconds even when TarInfo.mtime needs a float.
    Root extensions belong to the archive and are separate from this conversion.
    """
    if not isinstance(entry, Entry):
        raise TypeError("entry must be Entry")
    if entry.extensions:
        warnings.warn(
            "TarInfo omits Zstar manifest extensions; rewriting this metadata loses extensions",
            UserWarning,
            stacklevel=2,
        )
    info = TarInfo(entry.path)
    info.type = {
        EntryKind.FILE: REGTYPE,
        EntryKind.DIRECTORY: DIRTYPE,
        EntryKind.SYMLINK: SYMTYPE,
    }[entry.kind]
    info.size, info.mode, info.uid, info.gid = entry.size, entry.mode, entry.uid, entry.gid
    seconds, fraction = divmod(abs(entry.mtime_ns), 1_000_000_000)
    text = f"{'-' if entry.mtime_ns < 0 else ''}{seconds}.{fraction:09d}"
    info.mtime = entry.mtime_ns // 1_000_000_000 if fraction == 0 else float(text)
    info.pax_headers = {"mtime": text}
    info.linkname = entry.linkname or ""
    return info
