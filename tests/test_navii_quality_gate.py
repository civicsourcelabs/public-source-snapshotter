"""Tests for the Navii handoff quality gate."""

from __future__ import annotations

import unittest

from collectors.navii_detail.quality_gate import evaluate_quality


KINDS = ["hospital", "clinic", "dental", "pharmacy"]


def shard_metrics(
    *, zero_phone: bool = False, parse_errors: int = 0, unresolved: int = 0
) -> dict:
    kind_metrics = {}
    for kind in KINDS:
        kind_metrics[kind] = {
            "candidate_count": 25,
            "fetch_ok_count": 25,
            "fetch_error_count": 0,
            "parse_ok_count": 25 - parse_errors,
            "parse_error_count": parse_errors,
            "section_count": 50,
            "table_row_count": 100,
            "link_row_count": 25,
            "phone_number_row_count": 0 if zero_phone else 25,
            "unknown_count": 0,
            "fallback_count": 0,
            "resolution_unresolved_count": unresolved if kind == "clinic" else 0,
            "structure_fingerprints": ["sha256:fixture"],
        }
    return {
        "status": "executed",
        "candidate_count": 100,
        "fetch_ok_count": 100,
        "fetch_error_count": 0,
        "parse_ok_count": 100 - parse_errors * 4,
        "parse_error_count": parse_errors * 4,
        "section_count": 200,
        "table_rows": 400,
        "link_rows": 100,
        "phone_number_rows": 100 if not zero_phone else 0,
        "unresolved_count": unresolved,
        "kind_metrics": kind_metrics,
    }


class NaviiQualityGateTest(unittest.TestCase):
    def test_four_kind_canary_passes_only_with_required_extraction(self) -> None:
        result = evaluate_quality(
            [shard_metrics()],
            execute=True,
            candidate_mode="sample",
            sample_per_kind=25,
            kinds=KINDS,
            fail_on_fetch_error_rate=5,
        )

        self.assertEqual(result["quality_status"], "pass")
        self.assertEqual(result["gate"], "canary")
        self.assertTrue(all(result["per_kind"][kind]["quality_status"] == "pass" for kind in KINDS))

    def test_canary_fails_when_phone_extraction_is_zero(self) -> None:
        result = evaluate_quality(
            [shard_metrics(zero_phone=True)],
            execute=True,
            candidate_mode="sample",
            sample_per_kind=25,
            kinds=KINDS,
            fail_on_fetch_error_rate=5,
        )

        self.assertEqual(result["quality_status"], "fail")
        self.assertTrue(any("phone_number_row_count is zero" in reason for reason in result["failure_reasons"]))

    def test_canary_fails_when_parser_reports_an_error(self) -> None:
        result = evaluate_quality(
            [shard_metrics(parse_errors=1)],
            execute=True,
            candidate_mode="sample",
            sample_per_kind=25,
            kinds=KINDS,
            fail_on_fetch_error_rate=5,
        )

        self.assertEqual(result["quality_status"], "fail")
        self.assertTrue(any("parse errors detected" in reason for reason in result["failure_reasons"]))

    def test_preflight_uses_extraction_checks_without_fixed_sample_minimum(self) -> None:
        result = evaluate_quality(
            [shard_metrics()],
            execute=True,
            candidate_mode="preflight",
            sample_per_kind=0,
            kinds=KINDS,
            fail_on_fetch_error_rate=5,
        )

        self.assertEqual(result["quality_status"], "pass")
        self.assertEqual(result["gate"], "preflight")

    def test_dry_run_is_not_a_quality_pass(self) -> None:
        result = evaluate_quality(
            [],
            execute=False,
            candidate_mode="sample",
            sample_per_kind=25,
            kinds=KINDS,
            fail_on_fetch_error_rate=5,
        )

        self.assertEqual(result["quality_status"], "not_applicable")

    def test_one_percent_unresolved_is_allowed(self) -> None:
        result = evaluate_quality(
            [shard_metrics(unresolved=1)],
            execute=True,
            candidate_mode="preflight",
            sample_per_kind=0,
            kinds=KINDS,
            fail_on_fetch_error_rate=5,
            fail_on_unresolved_rate=1,
        )

        self.assertEqual(result["quality_status"], "pass")
        self.assertEqual(result["aggregate"]["unresolved_rate_percent"], 1.0)

    def test_unresolved_above_one_percent_fails(self) -> None:
        result = evaluate_quality(
            [shard_metrics(unresolved=2)],
            execute=True,
            candidate_mode="preflight",
            sample_per_kind=0,
            kinds=KINDS,
            fail_on_fetch_error_rate=5,
            fail_on_unresolved_rate=1,
        )

        self.assertEqual(result["quality_status"], "fail")
        self.assertTrue(any("unresolved rate" in reason for reason in result["failure_reasons"]))


if __name__ == "__main__":
    unittest.main()
