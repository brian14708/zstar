"""Entry-manifest metadata and selective reads through installed APIs.

The independent fixture generator owns expected bytes and corruption cases.
All operations use the current entry-manifest corpus.
"""

import hashlib
import json
from pathlib import Path

import pytest
import zstar
from zstar import _native

ROOT = Path(__file__).resolve().parents[2] / "testdata" / "entry-manifest"
MANIFEST = json.loads((ROOT / "manifest.json").read_text())
assert MANIFEST["schema_version"] == 2
assert MANIFEST["format"] == "entry-manifest"
CASES = [
    {
        **case,
        "archive": path,
        "archive_sha256": archive["sha256"],
    }
    for path, archive in MANIFEST["archives"].items()
    for case in archive["cases"]
]


def artifact(name):
    path = (ROOT / name).resolve()
    assert path.is_relative_to(ROOT.resolve()), name
    return path.read_bytes()


class Objects:
    def __init__(self, objects):
        self.objects = objects

    def get_encoded(self, address, hint, max_bytes):
        name = self.objects.get(address)
        if name is None:
            raise FileNotFoundError("corpus object absent")
        encoded = artifact(name)
        assert len(encoded) <= max_bytes
        return encoded


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_corpus(case, source_factory):
    data = artifact(case["archive"])
    assert hashlib.sha256(data).hexdigest() == case["archive_sha256"]
    try:
        with zstar.Archive.from_source(
            source_factory(data),
            len(data),
            resolver=Objects(case.get("objects", {})),
            verify_external=case.get("verify_external", False),
            **case.get("limits", {}),
        ) as archive:
            if "request" not in case:
                _ = archive.entry_count
            else:
                request = case["request"]
                actual, report = archive.read_with_report(
                    request["path"], request["offset"], request["length"]
                )
                assert actual == bytes.fromhex(case["range_result_hex"])
                assert report["metadata_validated"]
                if case.get("objects"):
                    checked = bool(report["external_objects_sha256_checked"])
                    unchecked = bool(report["external_objects_unchecked"])
                    assert checked == case.get("verify_external", False)
                    assert unchecked == (not checked)
    except zstar.ZstarError as error:
        assert error.category == case.get("expected_error"), str(error)
    else:
        assert case.get("expected_error") is None, "expected SDK rejection"


def test_error_categories_and_budgets():
    with pytest.raises(ValueError) as caught:
        zstar.Archive(b"invalid")
    assert isinstance(caught.value, zstar.ZstarError)
    assert caught.value.category == "not-zstar"
    archive = zstar.Archive(_native.pack_file("x", b"hello"), max_read_bytes=2)
    with pytest.raises(zstar.ZstarError) as caught:
        archive.read("x")
    assert caught.value.category == "resource-limit"
    assert archive.read("x", 1, 2) == b"el"
