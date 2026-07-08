"""Navii source readiness resolver tests."""

from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path

from collectors.navii_detail.source_readiness import (
    SOURCE_SPECS,
    expected_semiannual_snapshot_date,
    resolve_manifest,
    run_label_for,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "sources/navii/source-manifest.json"

HTML_FIXTURE = """
<html><body>
  <h3>2025年12月１日時点</h3>
  <a href="/content/11121000/05_pharmacy_20251201.zip">薬局</a>
  <h3>2026年６月１日時点</h3>
  <a href="/content/11121000/01-1_hospital_facility_info_20260601.zip">病院 (施設票)</a>
  <a href="/content/11121000/01-2_hospital_speciality_hours_20260601.zip">病院 (診療科・診療時間票）</a>
  <a href="/content/11121000/02-1_clinic_facility_info_20260601.zip">診療所 (施設票)</a>
  <a href="/content/11121000/02-2_clinic_speciality_hours_20260601.zip">診療所 (診療科・診療時間票）</a>
  <a href="/content/11121000/03-1_dental_facility_info_20260601.zip">歯科診療所 (施設票)</a>
  <a href="/content/11121000/03-2_dental_speciality_hours_20260601.zip">歯科診療所 (診療科・診療時間票）</a>
  <a href="/content/11121000/05_pharmacy_20260601.zip">薬局</a>
</body></html>
"""


class NaviiSourceReadinessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = {
            "schema_version": "1.0",
            "source_id": "navii_detail",
            "source_terms_url": "https://www.mhlw.go.jp/stf/example.html",
            "source_urls": [],
        }

    def test_expected_semiannual_snapshot_date_is_derived_from_jst_date(self) -> None:
        self.assertEqual(expected_semiannual_snapshot_date(date(2026, 1, 15)), "2025-12-01")
        self.assertEqual(expected_semiannual_snapshot_date(date(2026, 6, 5)), "2026-06-01")
        self.assertEqual(expected_semiannual_snapshot_date(date(2026, 11, 30)), "2026-06-01")
        self.assertEqual(expected_semiannual_snapshot_date(date(2026, 12, 8)), "2026-12-01")

    def test_resolve_manifest_uses_expected_official_snapshot(self) -> None:
        resolved = resolve_manifest(
            self.manifest,
            page_html=HTML_FIXTURE,
            expected_snapshot_date="2026-06-01",
        )

        self.assertEqual(resolved["source_snapshot_date"], "2026-06-01")
        self.assertEqual(len(resolved["source_urls"]), len(SOURCE_SPECS))
        self.assertEqual(resolved["resolution"]["source_url_count"], len(SOURCE_SPECS))
        self.assertTrue(
            all("20260601" in source["url"] for source in resolved["source_urls"]),
            resolved["source_urls"],
        )
        self.assertEqual(
            [source["kind"] for source in resolved["source_urls"]],
            [spec.kind for spec in SOURCE_SPECS],
        )

    def test_missing_expected_snapshot_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected source snapshot 2026-12-01"):
            resolve_manifest(
                self.manifest,
                page_html=HTML_FIXTURE,
                expected_snapshot_date="2026-12-01",
            )

    def test_missing_required_link_fails_closed(self) -> None:
        html_without_pharmacy = HTML_FIXTURE.replace(
            '<a href="/content/11121000/05_pharmacy_20260601.zip">薬局</a>',
            "",
        )
        with self.assertRaisesRegex(ValueError, "missing source links"):
            resolve_manifest(
                self.manifest,
                page_html=html_without_pharmacy,
                expected_snapshot_date="2026-06-01",
            )

    def test_run_label_is_derived_from_snapshot_date_and_mode(self) -> None:
        self.assertEqual(
            run_label_for("2026-06-01", "full"),
            "collector-navii-detail-20260601-full",
        )
        self.assertEqual(
            run_label_for("2026-12-01", "scope"),
            "collector-navii-detail-20261201-scope",
        )
        with self.assertRaisesRegex(ValueError, "invalid run label suffix"):
            run_label_for("2026-06-01", "manual")

    def test_manifest_template_matches_required_source_contract(self) -> None:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        source_urls = manifest["source_urls"]

        self.assertEqual(manifest["source_snapshot_date"], "auto")
        self.assertFalse(any("url" in source for source in source_urls))
        self.assertEqual(
            [(source["kind"], source["facility_kind"], source["expected_filename"], source["official_link_text"]) for source in source_urls],
            [
                (spec.kind, spec.facility_kind, spec.expected_filename, spec.normalized_label)
                for spec in SOURCE_SPECS
            ],
        )


if __name__ == "__main__":
    unittest.main()
