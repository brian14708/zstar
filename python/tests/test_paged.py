import pytest
from zstar import Archive, ZstarError


@pytest.mark.parametrize(
    "name",
    [
        "cross-page-ancestors",
        "unselected-extension-checksum",
        "unselected-entry-checksum",
        "unused-external-declaration",
    ],
)
def test_selected_metadata_and_complete_validation(name, corpus_dir):
    with Archive.from_file(corpus_dir / "pages" / f"{name}.zstar") as archive:
        assert archive.read("a/file", 0, 5) == bytes(range(5))
        assert archive.getmetadata("a/file").info.name == "a/file"
        if name == "cross-page-ancestors":
            assert archive.entry_count == 6
        else:
            with pytest.raises(ZstarError):
                archive.entry_count


def test_ancestor_on_another_page_must_be_a_directory(corpus_dir):
    with Archive.from_file(corpus_dir / "pages/missing-ancestor.zstar") as archive:
        with pytest.raises(ZstarError) as caught:
            archive.getmetadata("a/file")
        assert caught.value.category == "malformed-toc"
