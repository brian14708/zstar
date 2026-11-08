import pytest
from _support import open_file
from zstar import ZstarError


@pytest.mark.parametrize(
    "name",
    [
        "cross-page-ancestors",
        "unselected-extension-checksum",
        "unselected-entry-checksum",
        "unused-external-declaration",
    ],
)
@pytest.mark.parametrize("prefix", [None, "a/"])
def test_selected_metadata_and_complete_validation(name, corpus_dir, prefix):
    with open_file(corpus_dir / "pages" / f"{name}.zstar") as archive:
        assert archive.read("a/file", offset=0, length=5) == bytes(range(5))
        assert archive.entry("a/file").path == "a/file"
        if name == "cross-page-ancestors":
            assert len(list(archive.entries(prefix))) == (6 if prefix is None else 1)
        else:
            with pytest.raises(ZstarError):
                next(archive.entries(prefix))


def test_ancestor_on_another_page_must_be_a_directory(corpus_dir):
    with open_file(corpus_dir / "pages/missing-ancestor.zstar") as archive:
        with pytest.raises(ZstarError) as caught:
            archive.entry("a/file")
        assert caught.value.category == "malformed-toc"
