"""fsspec integration for reading members from Zstar archives."""

from __future__ import annotations

import errno
import posixpath
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from typing import BinaryIO, Literal, Protocol, TypedDict, cast, overload

try:
    from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
except ModuleNotFoundError as exc:  # pragma: no cover
    if exc.name != "fsspec":
        raise
    raise ImportError("install zstar-py[fsspec] to use ZstarFileSystem") from exc

from . import Archive
from ._archive import _link_target
from ._info import TarInfo
from ._source import _FileSource
from .types import ObjectResolver


class FileInfo(TypedDict):
    name: str
    size: int
    type: Literal["file", "directory"]


class _Backend(Protocol):
    def info(self, path: str) -> dict[str, object]: ...
    def open(self, path: str, mode: str, **kwargs: object) -> BinaryIO: ...
    def cat_file(self, path: str, *, start: int, end: int, **kwargs: object) -> bytes: ...


def _close_after_failure(resource: BinaryIO, primary: BaseException) -> None:
    try:
        resource.close()
    except BaseException as cleanup:
        raise BaseExceptionGroup("operation and cleanup failed", [primary, cleanup]) from None


def _file_source(file: BinaryIO) -> _FileSource:
    try:
        return _FileSource(file)
    except BaseException as primary:
        _close_after_failure(file, primary)
        raise


def _strong_etag(tag: str) -> bool:
    return (
        len(tag) >= 2
        and tag[0] == tag[-1] == '"'
        and all(
            byte == 0x21 or 0x23 <= byte <= 0x7E or byte >= 0x80
            for byte in tag[1:-1].encode("utf-8")
        )
    )


class _FsspecSource:
    """S3 ranges pinned by a version or enforced IfMatch condition."""

    def __init__(self, backend: _Backend, path: str) -> None:
        self.fs, self.path = backend, path
        info = backend.info(path)
        self.size = int(cast(int, info["size"]))
        self.file: BinaryIO | None = None
        self._reader: _FileSource | None = None
        version, etag = info.get("VersionId"), info.get("ETag")
        if isinstance(version, str) and version not in ("", "null"):
            self.condition: dict[str, object] = {"version_id": version}
        elif isinstance(etag, str) and _strong_etag(etag):
            # s3fs.cat_file does not accept IfMatch. Its S3File range reader
            # enforces the ETag recorded at open; retain that handle and reject
            # configurations that disable conditional reads.
            file = backend.open(path, "rb", cache_type="none", block_size=0)
            try:
                if getattr(file, "read_kw", {}).get("IfMatch") != etag:
                    raise OSError("S3 file does not enforce the pinned ETag")
                self._reader = _file_source(file)
                self.file = file
            except BaseException as primary:
                if not file.closed:
                    _close_after_failure(file, primary)
                raise
            self.condition = {}
        else:
            raise OSError("S3 archive requires an immutable version or ETag")

    def read_range(self, offset: int, length: int) -> bytes:
        if self._reader is not None:
            return self._reader.read_range(offset, length)
        return self.fs.cat_file(self.path, start=offset, end=offset + length, **self.condition)


class _Slot:
    """One loaded archive generation. Pins belong to the slot, never to a path."""

    def __init__(self, path: str, archive: Archive, source: _FileSource | _FsspecSource | None):
        self.path, self.archive, self.source = path, archive, source
        self.pins = 0

    def close(self) -> None:
        errors: list[BaseException] = []
        file = self.source.file if self.source is not None else None
        for resource in (self.archive, file):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    errors.append(error)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("archive cleanup failed", errors)


class _Loading:
    def __init__(self) -> None:
        self.done = threading.Event()


class _MemberFile(AbstractBufferedFile):
    fs: ZstarFileSystem

    def __init__(self, fs: ZstarFileSystem, path: str, slot: _Slot, **kwargs: object) -> None:
        self._lease_released = True
        self._slot = slot
        self._archive = slot.archive
        archive_path, requested = fs._split(path)
        self._archive_path = archive_path
        self._member = fs._resolve(slot.archive, requested).name
        entry = slot.archive.getmember(self._member)
        if not entry.isfile():
            raise IsADirectoryError(path)
        self._size = entry.size
        stream = slot.archive.extractfile(self._member)
        assert stream is not None
        self._stream = stream
        super().__init__(fs, path, mode="rb", size=self._size, **kwargs)
        self._lease_released = False

    def close(self) -> None:
        try:
            super().close()
        finally:
            if hasattr(self, "_stream"):
                self._stream.close()
            if not self._lease_released:
                self._lease_released = True
                self.fs._release(self._slot)

    def _fetch_range(self, start: int, end: int) -> bytes:
        self._stream.seek(start)
        output = bytearray(end - start)
        count = self._stream.readinto(output)
        return bytes(memoryview(output)[:count])


class ZstarFileSystem(AbstractFileSystem):
    """Expose members using ``archive.zstar!/member`` paths.

    ``opener`` returns archive bytes or a readable binary object. It may wrap
    local files or remote snapshots; its returned source must stay immutable.

    Thread safety: one instance may be shared between threads. A lock protects
    the archive cache and pin counts; the opener runs outside it, and
    concurrent requests for the same archive share one load. Open member files
    pin their archive generation so cache eviction never closes an archive in
    use. ``close()`` is terminal, invalidates open members, and drains admitted
    loads before returning. Distinct pending loads are bounded by cache capacity.
    """

    protocol = "zstar"

    def __init__(
        self,
        opener: Callable[[str], bytes | bytearray | BinaryIO] | None = None,
        storage_options: Mapping[str, object] | None = None,
        max_open_archives: int = 8,
        cache_bytes: int = 16 << 20,
        verify_external: bool = True,
        resolver: ObjectResolver | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        if not isinstance(max_open_archives, int) or max_open_archives < 1:
            raise ValueError("max_open_archives must be a positive integer")
        if not isinstance(cache_bytes, int) or cache_bytes < 0:
            raise ValueError("cache_bytes must be a nonnegative integer")
        self.max_open_archives = max_open_archives
        self.cache_bytes = cache_bytes
        self.verify_external = verify_external
        self.resolver = resolver
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._closed = False
        self._closing = False
        self.opener = opener
        # fsspec overwrites its public storage_options after construction.
        self._backend_options = dict(storage_options or {})
        self._archives: OrderedDict[str, _Slot] = OrderedDict()
        self._loading: dict[str, _Loading] = {}

    def _evictable(self, keep: str | None) -> _Slot | None:
        for slot in self._archives.values():
            if slot.pins == 0 and slot.path != keep:
                return slot
        return None

    def _acquire(self, archive_path: str) -> _Slot:
        """Return the cached slot for a path, loading it once; the slot is pinned."""
        while True:
            with self._lock:
                if self._closed:
                    raise ValueError("filesystem closed")
                slot = self._archives.get(archive_path)
                if slot is not None:
                    self._archives.move_to_end(archive_path)
                    slot.pins += 1
                    return slot
                pending = self._loading.get(archive_path)
                if pending is None:
                    if len(self._loading) >= self.max_open_archives:
                        self._condition.wait()
                        continue
                    # Decide up front, so a failing opener never evicts a good archive.
                    if len(self._archives) >= self.max_open_archives and not self._evictable(None):
                        raise OSError(
                            "all cached archives have open members; "
                            "close a member before opening another archive"
                        )
                    self._loading[archive_path] = pending = _Loading()
                    break
            pending.done.wait()
        try:
            slot = self._open_slot(archive_path)
        except BaseException:
            with self._lock:
                del self._loading[archive_path]
                self._condition.notify_all()
            pending.done.set()
            raise
        victims: list[_Slot] = []
        published = False
        try:
            with self._lock:
                if self._closed:
                    raise ValueError("filesystem closed")
                while len(self._archives) >= self.max_open_archives:
                    victim = self._evictable(archive_path)
                    if victim is None:
                        raise OSError(
                            "all cached archives have open members; "
                            "close a member before opening another archive"
                        )
                    del self._archives[victim.path]
                    victims.append(victim)
                slot.pins = 1
                self._archives[archive_path] = slot
                published = True
            self._close_slots(victims)
            return slot
        except BaseException as primary:
            if published:
                self._release(slot)
                raise
            try:
                slot.close()
            except BaseException as cleanup:
                raise BaseExceptionGroup(
                    "operation and cleanup failed", [primary, cleanup]
                ) from None
            raise
        finally:
            with self._lock:
                del self._loading[archive_path]
                self._condition.notify_all()
            pending.done.set()

    def _open_slot(self, archive_path: str) -> _Slot:
        source: _FileSource | _FsspecSource | None = None
        if self.opener is None:
            from fsspec.core import url_to_fs

            if archive_path.startswith(("http://", "https://")):
                unknown = set(self._backend_options) - {"headers"}
                if unknown:
                    raise ValueError("HTTP storage_options supports headers only")
                archive = Archive.from_url(
                    archive_path,
                    headers=cast("dict[str, str] | None", self._backend_options.get("headers")),
                    cache_bytes=self.cache_bytes,
                    verify_external=self.verify_external,
                    resolver=self.resolver,
                )
                return _Slot(archive_path, archive, None)
            backend, path = url_to_fs(archive_path, **self._backend_options)
            protocol = backend.protocol
            protocols = (protocol,) if isinstance(protocol, str) else protocol
            if "file" in protocols or "local" in protocols:
                archive = Archive.open(
                    path,
                    cache_bytes=self.cache_bytes,
                    verify_external=self.verify_external,
                    resolver=self.resolver,
                )
                return _Slot(archive_path, archive, None)
            if "http" in protocols or "https" in protocols:
                headers = self._backend_options.get("headers")
                archive = Archive.from_url(
                    path,
                    headers=cast("dict[str, str] | None", headers),
                    cache_bytes=self.cache_bytes,
                    verify_external=self.verify_external,
                    resolver=self.resolver,
                )
                return _Slot(archive_path, archive, None)
            if "s3" not in protocols:
                raise OSError(
                    "backend cannot guarantee an immutable archive snapshot; "
                    "supply an opener for an immutable source"
                )
            source = _FsspecSource(cast(_Backend, backend), path)
        else:
            value = self.opener(archive_path)
            if isinstance(value, (bytes, bytearray)):
                archive = Archive(
                    value,
                    cache_bytes=self.cache_bytes,
                    verify_external=self.verify_external,
                    resolver=self.resolver,
                )
                try:
                    archive.verify()
                except BaseException:
                    archive.close()
                    raise
                return _Slot(archive_path, archive, None)
            source = _file_source(value)
        try:
            archive = Archive.from_source(
                source,
                source.size,
                cache_bytes=self.cache_bytes,
                verify_external=self.verify_external,
                resolver=self.resolver,
            )
        except BaseException as primary:
            if source.file is not None:
                _close_after_failure(source.file, primary)
            raise
        return _Slot(archive_path, archive, source)

    @staticmethod
    def _close_slots(slots: list[_Slot]) -> None:
        errors: list[BaseException] = []
        for slot in slots:
            try:
                slot.close()
            except BaseException as error:
                errors.append(error)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("archive cleanup failed", errors)

    def _release(self, slot: _Slot) -> None:
        with self._lock:
            if slot.pins > 0:
                slot.pins -= 1

    @contextmanager
    def _lease(self, archive_path: str) -> Iterator[Archive]:
        slot = self._acquire(archive_path)
        try:
            yield slot.archive
        finally:
            self._release(slot)

    def _load(self, archive_path: str) -> Archive:
        """Return a cached archive without pinning it (tests and diagnostics)."""
        slot = self._acquire(archive_path)
        self._release(slot)
        return slot.archive

    def close(self) -> None:
        with self._lock:
            if self._closed:
                while self._closing:
                    self._condition.wait()
                return
            self._closed = True
            self._closing = True
            self._condition.notify_all()
            slots = list(self._archives.values())
            self._archives.clear()
        try:
            self._close_slots(slots)
        finally:
            with self._lock:
                while self._loading:
                    self._condition.wait()
                self._closing = False
                self._condition.notify_all()

    @staticmethod
    def _split(path: str) -> tuple[str, str]:
        archive, sep, member = path.rpartition("!/")
        if not sep:
            # ``a.zstar!`` names the archive root; fsspec strips the slash.
            if path.endswith("!"):
                return path[:-1], ""
            raise ValueError("path must be archive.zstar!/member")
        return archive, member.lstrip("/")

    @classmethod
    def _strip_protocol(cls, path: str | list[str]) -> str | list[str]:
        stripped = super()._strip_protocol(path)
        if isinstance(stripped, str) and stripped.endswith("!"):
            return stripped + "/"
        return cast("str | list[str]", stripped)

    @staticmethod
    def _resolve(archive: Archive, member: str) -> TarInfo:
        """Resolve final and ancestor links within the archive, at most 40 hops."""
        current = member
        visited: set[str] = set()
        for _ in range(41):
            if current in visited:
                break
            visited.add(current)
            parts = current.split("/")
            for i in range(len(parts)):
                path = "/".join(parts[: i + 1])
                try:
                    info = archive.getmember(path)
                except KeyError:
                    raise FileNotFoundError(member) from None
                if info.issym():
                    current = posixpath.normpath(
                        posixpath.join(_link_target(info), *parts[i + 1 :])
                    )
                    break
                if i == len(parts) - 1:
                    return info
                if not info.isdir():
                    raise NotADirectoryError(member)
            else:  # pragma: no cover
                raise FileNotFoundError(member)
        raise OSError(errno.ELOOP, "cyclic archive symlink or too many links", member)

    def _open(self, path: str, mode: str = "rb", **kwargs: object) -> _MemberFile:
        if mode != "rb":
            raise NotImplementedError("ZstarFileSystem is read-only")
        archive_path, member = self._split(path)
        slot = self._acquire(archive_path)
        try:
            return _MemberFile(self, f"{archive_path}!/{member}", slot, **kwargs)
        except BaseException:
            self._release(slot)
            raise

    def info(self, path: str, **kwargs: object) -> FileInfo:
        archive_path, member = self._split(cast(str, self._strip_protocol(path)))
        with self._lease(archive_path) as archive:
            if not member.rstrip("/"):
                return {"name": f"{archive_path}!/", "type": "directory", "size": 0}
            member = member.rstrip("/")
            entry = self._resolve(archive, member)
            return {
                "name": f"{archive_path}!/{member}",
                "size": entry.size,
                "type": "directory" if entry.isdir() else "file",
            }

    @overload
    def ls(self, path: str, detail: Literal[True] = True, **kwargs: object) -> list[FileInfo]: ...

    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: object) -> list[str]: ...

    @overload
    def ls(self, path: str, detail: bool, **kwargs: object) -> list[FileInfo] | list[str]: ...

    def ls(self, path: str, detail: bool = True, **kwargs: object) -> list[FileInfo] | list[str]:
        archive_path, member = self._split(cast(str, self._strip_protocol(path)).rstrip("/") + "/")
        member = member.rstrip("/")
        with self._lease(archive_path) as archive:
            requested = member
            if member:
                entry = self._resolve(archive, member)
                if not entry.isdir():
                    info: FileInfo = {
                        "name": f"{archive_path}!/{member}",
                        "type": "file",
                        "size": entry.size,
                    }
                    return [info] if detail else [info["name"]]
                member = entry.name
            prefix = member + "/" if member else ""
            display_prefix = requested + "/" if requested else ""
            if not detail:
                return [
                    f"{archive_path}!/{display_prefix}{name[len(prefix) :]}"
                    for entry in archive._iter_prefix(prefix)
                    if "/" not in (name := entry.name)[len(prefix) :]
                ]
            items: list[FileInfo] = []
            for entry in archive._iter_prefix(prefix):
                if "/" in entry.name[len(prefix) :]:
                    continue
                target = entry
                if entry.issym():
                    # Dangling links list as the link itself.
                    with suppress(OSError):
                        target = self._resolve(archive, entry.name)
                items.append(
                    {
                        "name": f"{archive_path}!/{display_prefix}{entry.name[len(prefix) :]}",
                        "size": target.size,
                        "type": "directory" if target.isdir() else "file",
                    }
                )
            return items
