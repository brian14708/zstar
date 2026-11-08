"""Archive member kinds shared by native metadata and annotations."""

from enum import StrEnum


class EntryKind(StrEnum):
    """The three supported archive member kinds."""

    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
