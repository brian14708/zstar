"""Small fixtures shared by the installed-package tests."""

from pathlib import Path

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "free_threading: concurrency stress tests; meaningful on any build, "
        "and the main check on free-threaded CPython (run with PYTHON_GIL=0)",
    )


@pytest.fixture(scope="session")
def corpus_dir():
    return Path(__file__).resolve().parents[2] / "testdata/entry-manifest"


@pytest.fixture(scope="session")
def inline_data(corpus_dir):
    return (corpus_dir / "inline.zstar").read_bytes()


@pytest.fixture(scope="session")
def thin_data(corpus_dir):
    return (corpus_dir / "thin.zstar").read_bytes()


class MemorySource:
    def __init__(self, data):
        self.data = data
        self.calls = 0
        self.closed = False

    def read_range(self, offset, length):
        self.calls += 1
        return self.data[offset : offset + length]

    def close(self):
        self.closed = True


@pytest.fixture
def source_factory(inline_data):
    return lambda data=inline_data: MemorySource(data)


class ObjectResolver:
    def __init__(self, corpus_dir):
        self.directory = corpus_dir / "objects"
        self.calls = 0

    def get_encoded(self, address, hint, max_bytes):
        self.calls += 1
        encoded = (self.directory / f"{address[7:]}.zst").read_bytes()
        assert len(encoded) <= max_bytes
        return encoded


@pytest.fixture
def object_resolver(corpus_dir):
    return ObjectResolver(corpus_dir)
