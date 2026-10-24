"""Entry-manifest operations and verification scopes through installed APIs.

The independent fixture generator owns expected bytes and corruption cases.
All operations use the current entry-manifest corpus.
"""

import hashlib
import json
from pathlib import Path

import pytest
import zstar
import zstar.format

ROOT = Path(__file__).resolve().parents[3] / "testdata" / "entry-manifest"
MANIFEST = json.loads((ROOT / "manifest.json").read_text())
assert MANIFEST["schema_version"] == 1
assert MANIFEST["format"] == "entry-manifest"


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


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda case: case["name"])
def test_corpus(case, source_factory):
    data = artifact(case["archive"])
    assert hashlib.sha256(data).hexdigest() == case["archive_sha256"]
    report = {}
    try:
        operation = case["operation"]
        options = case.get("limits", {})
        if operation in ("inline-range", "external-range"):
            archive = zstar.format.IndexedArchive(
                source_factory(data),
                len(data),
                verify_external=case.get("verify_external", True),
                **options,
            )
            request = case["request"]
            resolver = Objects(case.get("objects", {})) if operation == "external-range" else None
            actual, report = archive.read_with_report(
                request["path"], request["offset"], request["length"], resolver=resolver
            )
            assert actual == artifact(case["range_result"])
            assert report["metadata_authenticated"]
            assert not report["full_archive_validated"]
            if operation == "external-range":
                if case.get("verify_external", True):
                    assert report["external_objects_sha256_checked"]
                    assert not report["external_objects_unchecked"]
                else:
                    assert report["external_objects_unchecked"]
                    assert not report["external_objects_sha256_checked"]
        elif operation == "metadata":
            archive = zstar.format.IndexedArchive(source_factory(data), len(data), **options)
            archive.validate_metadata()
        else:
            resolver = Objects(case.get("objects", {})) if operation == "external-closure" else None
            archive = zstar.Archive(data, resolver=resolver, **options)
            report = archive.verify(external=operation == "external-closure")
            for path, expected in case.get("members", {}).items():
                assert archive.read(path, resolver=resolver) == artifact(expected)
            for path, target in case.get("links", {}).items():
                assert archive.getmember(path).linkname == target
    except zstar.ZstarError as error:
        assert error.category == case["expected_error"], str(error)
    else:
        assert case["expected_error"] is None, "expected SDK rejection"
        for key, expected in case.get("expected_verification", {}).items():
            actual = report[key]
            if key == "external_objects_sha256_checked" and isinstance(expected, bool):
                actual = bool(actual)
            assert actual == expected, key


def test_error_compatibility_and_budgets():
    with pytest.raises(ValueError) as caught:
        zstar.Archive(b"invalid")
    assert isinstance(caught.value, zstar.ZstarError)
    assert caught.value.category == "not-zstar"
    archive = zstar.Archive(zstar.pack_file("x", b"hello"), max_read_bytes=2)
    with pytest.raises(zstar.ZstarError) as caught:
        archive.read("x")
    assert caught.value.category == "resource-limit"
    assert archive.read("x", 1, 2) == b"el"
