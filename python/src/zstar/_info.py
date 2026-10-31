"""Convert native entries to standard tarfile metadata."""

from decimal import Decimal, InvalidOperation
from tarfile import DIRTYPE, REGTYPE, SYMTYPE, TarInfo
from typing import Literal

from .types import CborValue, EntryFields, EntryTuple


def _nanoseconds(value: int | float | str) -> int:
    try:
        seconds = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("mtime must be a number") from exc
    if not seconds.is_finite():
        raise ValueError("mtime must be finite")
    parts = seconds.as_tuple()
    assert isinstance(parts.exponent, int)
    result = Decimal((parts.sign, parts.digits, parts.exponent + 9))
    if result != result.to_integral_value():
        raise ValueError("mtime must have nanosecond precision")
    # Check the Decimal before conversion so extreme exponents cannot allocate
    # an unbounded Python integer for a timestamp that will be rejected.
    if not -(1 << 63) <= result < 1 << 63:
        raise OverflowError("mtime is outside the archive timestamp range")
    return int(result)


def _from_entry(entry: EntryTuple) -> TarInfo:
    name, kind, mode, uid, gid, mtime_ns, extents = entry[:7]
    info = TarInfo(name)
    info.type = {0: REGTYPE, 5: DIRTYPE, 2: SYMTYPE}[kind]
    info.size = sum(extent[0] for extent in extents)
    info.mode, info.uid, info.gid = mode, uid, gid
    seconds, fraction = divmod(abs(mtime_ns), 1_000_000_000)
    text = f"{'-' if mtime_ns < 0 else ''}{seconds}.{fraction:09d}"
    info.mtime = mtime_ns // 1_000_000_000 if fraction == 0 else float(text)
    info.pax_headers = {"mtime": text}
    fields = dict(entry[7]) if len(entry) == 8 else {}
    linkname = fields.pop("linkname", "")
    assert isinstance(linkname, str)
    info.linkname = linkname
    return info


def _to_entry(info: TarInfo) -> EntryTuple:
    if not isinstance(info, TarInfo):
        raise TypeError("tarinfo must be a tarfile.TarInfo")
    kind: Literal[0, 2, 5]
    if info.isfile():
        kind = 0
    elif info.isdir():
        kind = 5
    elif info.issym():
        kind = 2
    else:
        raise ValueError("only regular files, directories, and symlinks are supported")
    fields: dict[str, CborValue] = {}
    if info.issym():
        fields["linkname"] = info.linkname
    entry: EntryFields = (
        info.name,
        kind,
        info.mode,
        info.uid,
        info.gid,
        _nanoseconds(info.pax_headers.get("mtime", info.mtime)),
        (),
    )
    return (*entry, fields) if fields else entry
