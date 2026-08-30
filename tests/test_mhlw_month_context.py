"""MHLW month-context filename contract tests."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date
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

    def test_latest_available_source_snapshot_uses_newest_public_month(self) -> None:
        html = """
        <html><body>
          <p>令和8年8月1日現在</p>
          <p>（2）エクセルファイル</p>
          <a href="2608-06_01-01.zip">届出受理医療機関名簿（医科）［ZIP形式］</a>
          <p>令和8年6月1日現在</p>
          <a href="2606-06_10-01.zip">届出受理医療機関名簿（医科）［ZIP形式］</a>
        </body></html>
        """
        row = self.source_row("medical")
        args = type(
            "Args",
            (),
            {
                "timeout_seconds": 5,
                "retry_count": 0,
                "retry_backoff_seconds": 0,
                "user_agent": COLLECTOR.DEFAULT_USER_AGENT,
                "insecure_skip_tls_verify": False,
            },
        )()

        original_request_text = COLLECTOR.request_text
        try:
            COLLECTOR.request_text = lambda _url, *, args: html
            self.assertEqual(
                COLLECTOR.latest_available_source_snapshot_date(
                    [row], args=args, as_of_date=date(2026, 8, 31)
                ),
                "2026-08-01",
            )
        finally:
            COLLECTOR.request_text = original_request_text


if __name__ == "__main__":
    unittest.main()
