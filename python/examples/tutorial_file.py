# /// script
# requires-python = ">=3.11"
# dependencies = ["zstar-py>=0.0.1"]
# ///

"""Write an archive to a file, read its contents, and verify it."""

from zstar import Archive, ArchiveWriter


def main() -> None:
    # region create
    with ArchiveWriter.from_file("example.zstar") as writer:
        writer.write_bytes("example.txt", b"Hello from Zstar\n")
    # endregion create

    # region read
    with Archive.from_file("example.zstar") as archive:
        content = archive.read("example.txt")
        selected = archive.read_range("example.txt", 0, 5)
        assert content == b"Hello from Zstar\n"
        assert selected == b"Hello"
        print(content.decode(), end="")
        print(selected.decode())
        # endregion read
        # region verify
        archive.verify()
        print("Archive verified")
        # endregion verify


if __name__ == "__main__":
    main()
