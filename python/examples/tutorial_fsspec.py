# /// script
# requires-python = ">=3.11"
# dependencies = ["zstar-py>=0.0.1"]
# ///

"""Create an archive and read its members through fsspec."""

import fsspec


def main() -> None:
    # region create
    with fsspec.filesystem("zstar", fo="example-fsspec.zstar", mode="w") as fs:
        with fs.open("docs/hello.txt", "wt", encoding="utf-8") as member:
            member.write("Hello from Zstar\n")
        fs.pipe_file("data.bin", b"\x00\x01\x02")
    # endregion create

    # region read
    with fsspec.filesystem("zstar", fo="example-fsspec.zstar") as fs:
        print(fs.ls("docs", detail=False))
        with fs.open("docs/hello.txt", "rt", encoding="utf-8") as member:
            text = member.read()
        assert text == "Hello from Zstar\n"
        assert fs.cat_file("data.bin") == b"\x00\x01\x02"
        print(text, end="")
    # endregion read

    # region url
    fs, path = fsspec.core.url_to_fs("zstar://docs/hello.txt::example-fsspec.zstar")
    with fs, fs.open(path, "rb") as member:
        assert member.read(5) == b"Hello"
        print("Read through a chained URL")
    # endregion url


if __name__ == "__main__":
    main()
