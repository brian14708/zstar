# /// script
# requires-python = ">=3.11"
# dependencies = ["zstar-py>=0.0.1"]
# ///

"""Write an archive to a file, read its contents."""

import zstar


def main() -> None:
    # region create
    with zstar.open("example.zstar", mode="w") as writer:
        writer.writestr("example.txt", b"Hello from Zstar\n")
    # endregion create

    # region read
    with zstar.open("example.zstar") as archive:
        content = archive.read("example.txt")
        selected = archive.read("example.txt", offset=0, length=5)
        assert content == b"Hello from Zstar\n"
        assert selected == b"Hello"
        print(content.decode(), end="")
        print(selected.decode())
        # endregion read


if __name__ == "__main__":
    main()
