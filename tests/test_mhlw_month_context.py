"""MHLW month-context filename contract tests."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
COLLECTOR_PATH = REPO_ROOT / "collectors/mhlw_monthly/collect.py"
SPEC = importlib.util.spec_from_file_location("mhlw_monthly_collect", COLLECTOR_PATH)
assert SPEC and SPEC.loader
COLLECTOR = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = COLLECTOR
SPEC.loader.exec_module(COLLECTOR)


class MhlwMonthContextFilenameTest(unittest.TestCase):
    def source_row(self, pipeline_slug: str) -> object:
        return COLLECTOR.SourceRow(
            source_key=f"test-{pipeline_slug}",
            pipeline_slug=pipeline_slug,
            region="東海北陸",
            source_label="届出受理_test",
            source_type="todokede",
            fetch_type="month_context",
            download_subdir=f"東海北陸/届出受理/{pipeline_slug}",
            expected_filename="",
            page_url="https://example.invalid/monthly.html",
        )

    def test_accepts_july_and_august_publisher_sequences(self) -> None:
        cases = (
            ("medical", "2026-07", "2607-06_10-01.zip"),
            ("medical", "2026-08", "2608-06_01-01.zip"),
            ("dental", "2026-08", "2608-06_01-03.zip"),
            ("pharmacy", "2026-08", "2608-06_01-04.zip"),
        )
        for pipeline_slug, target_month, basename in cases:
            with self.subTest(basename=basename):
                COLLECTOR.validate_month_context_filename(
                    self.source_row(pipeline_slug),
                    basename=basename,
                    target_month=target_month,
                )

    def test_rejects_wrong_month_source_family_and_product(self) -> None:
        invalid_basenames = (
            "2607-06_01-01.zip",
            "2608-01_01-01.zip",
            "2608-06_01-03.zip",
            "2608-06_1-01.zip",
            "2608-06_001-01.zip",
        )
        for basename in invalid_basenames:
            with self.subTest(basename=basename):
                with self.assertRaisesRegex(RuntimeError, "did not match month_context pattern"):
                    COLLECTOR.validate_month_context_filename(
                        self.source_row("medical"),
                        basename=basename,
                        target_month="2026-08",
                    )


if __name__ == "__main__":
    unittest.main()
