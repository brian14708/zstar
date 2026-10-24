"""Exercise unified release selection before starting expensive package builds."""

import unittest
from unittest.mock import patch

from package_family import select


class ReleaseSelectionTests(unittest.TestCase):
    def test_root_tag_builds_every_family(self):
        with patch("package_family.version", return_value="0.0.1"):
            self.assertEqual(select("push", "refs/tags/v0.0.1", ""), "all")

    def test_manual_selection(self):
        with patch("package_family.version", return_value="0.0.1"):
            for family in ("all", "rust", "python", "go"):
                with self.subTest(family=family):
                    self.assertEqual(select("workflow_dispatch", "refs/heads/main", family), family)

    def test_invalid_events_tags_and_families(self):
        with patch("package_family.version", return_value="0.0.1"):
            for event, ref, family in (
                ("pull_request", "refs/heads/main", "all"),
                ("push", "refs/tags/v0.0.2", ""),
                ("push", "refs/tags/rust/v0.0.1", ""),
                ("push", "refs/tags/python/v0.0.1", ""),
                ("push", "refs/heads/v0.0.1", ""),
                ("push", "refs/tags/v0.0.1-extra", ""),
                ("workflow_dispatch", "refs/heads/main", "invalid"),
            ):
                with (
                    self.subTest(event=event, ref=ref, family=family),
                    self.assertRaises(ValueError),
                ):
                    select(event, ref, family)

    def test_mismatched_manifests_fail_for_tagged_and_manual_builds(self):
        with patch(
            "package_family.version",
            side_effect=lambda family: {"rust": "0.0.1", "python": "0.0.2"}[family],
        ):
            for event, ref, family in (
                ("push", "refs/tags/v0.0.1", ""),
                ("workflow_dispatch", "refs/heads/main", "all"),
                ("workflow_dispatch", "refs/heads/main", "go"),
            ):
                with (
                    self.subTest(event=event, family=family),
                    self.assertRaisesRegex(ValueError, "versions must match"),
                ):
                    select(event, ref, family)


if __name__ == "__main__":
    unittest.main()
