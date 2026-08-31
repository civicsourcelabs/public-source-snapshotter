"""Navii workflow automation contract tests."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = REPO_ROOT / ".github/workflows/navii-detail-snapshot.yml"
COLLECTOR_PATH = REPO_ROOT / "collectors/navii_detail/collect.py"


class NaviiWorkflowContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
        cls.collector = COLLECTOR_PATH.read_text(encoding="utf-8")

    def test_dispatch_inputs_do_not_reintroduce_manual_snapshot_metadata(self) -> None:
        dispatch_match = re.search(
            r"workflow_dispatch:\n(?P<body>.*?)\npermissions:",
            self.workflow,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(dispatch_match)
        dispatch_body = dispatch_match.group("body")

        self.assertNotIn("source_snapshot_date:", dispatch_body)
        self.assertNotIn("run_label:", dispatch_body)
        self.assertNotIn("inputs.source_snapshot_date", self.workflow)
        self.assertNotIn("inputs.run_label", self.workflow)

    def test_schedule_is_monthly_on_the_fifth_jst(self) -> None:
        self.assertIn('cron: "30 15 4 * *"', self.workflow)
        self.assertNotIn("4,7,10,13,16,19,22,25,28 6,12", self.workflow)
        self.assertNotIn("3,10,17,24,31 1,7", self.workflow)

    def test_workflow_resolves_latest_available_snapshot_and_run_label(self) -> None:
        self.assertNotIn("expected_monthly_snapshot_date()", self.workflow)
        self.assertNotIn("expected_semiannual_snapshot_date", self.workflow)
        self.assertIn("resolve_manifest(", self.workflow)
        self.assertIn('page_html=load_html(', self.workflow)
        self.assertIn('insecure_skip_tls_verify=insecure_skip_tls_verify == "true"', self.workflow)
        self.assertIn('resolved_source_manifest["source_snapshot_date"]', self.workflow)
        self.assertIn("def run_label_suffix(resolved: dict[str, str]) -> str:", self.workflow)
        self.assertIn('resolved["run_label"] = run_label_for(', self.workflow)
        self.assertIn('"source_snapshot_date": source_snapshot_date', self.workflow)

    def test_scheduled_poll_skips_existing_full_artifact(self) -> None:
        self.assertIn("actions: read", self.workflow)
        self.assertIn("GITHUB_TOKEN: ${{ github.token }}", self.workflow)
        self.assertIn("def successful_artifact_exists(run_label: str) -> bool:", self.workflow)
        self.assertIn("actions/artifacts?per_page=100&page=", self.workflow)
        self.assertIn('run_payload.get("head_sha") == current_sha', self.workflow)
        self.assertIn("skip_existing: ${{ steps.plan.outputs.skip_existing }}", self.workflow)
        self.assertEqual(
            len(re.findall(r"^\s+if: .*skip_existing != 'true'", self.workflow, flags=re.MULTILINE)),
            5,
        )

    def test_scheduled_run_stays_full_encrypted_snapshot(self) -> None:
        expected_pairs = {
            '"execute": "true"': "scheduled run must execute",
            '"confirm": "owner-approved-public-source-snapshot"': "scheduled run needs owner gate token",
            '"source_id": "navii_detail"': "scheduled run must keep source id",
            '"artifact_mode": "encrypted_full"': "scheduled run must upload encrypted full package",
            '"kinds": "hospital,clinic,dental,pharmacy"': "scheduled run must include all kinds",
            '"candidate_mode": "all"': "scheduled run must collect all candidates",
            '"shard_count": "16"': "scheduled run must use full shard count",
            '"max_pages_per_shard": "0"': "scheduled run must not page-limit full runs",
        }
        for snippet, message in expected_pairs.items():
            self.assertIn(snippet, self.workflow, message)

    def test_artifact_uses_derived_run_label_and_resolved_source_manifest(self) -> None:
        self.assertIn(
            "name: navii-detail-handoff-${{ needs.validate.outputs.run_label }}-${{ github.run_id }}",
            self.workflow,
        )
        self.assertIn("collectors/navii_detail/source_readiness.py", self.workflow)
        self.assertIn("source-snapshot-manifest.json", self.workflow)
        self.assertIn("readiness_args+=(--insecure-skip-tls-verify)", self.workflow)
        self.assertNotIn("cp sources/navii/source-manifest.json", self.workflow)

    def test_collector_does_not_freeze_open_data_csv_date(self) -> None:
        self.assertIn("OPEN_DATA_FILE_TEMPLATES", self.collector)
        self.assertIn("{yyyymmdd}", self.collector)
        self.assertIn('missing.append("--source-snapshot-date")', self.collector)
        self.assertNotIn("OPEN_DATA_FILES", self.collector)
        self.assertNotIn("20251201", self.collector)
        self.assertNotIn("2025-12-01", self.collector)

    def test_navii_collector_uses_dom_parser_and_fails_closed_on_parse_errors(self) -> None:
        self.assertIn("NAVII_PARSER_VERSION = \"dom-v2\"", self.collector)
        self.assertIn("def discover_sections", self.collector)
        self.assertIn("aria-controls", self.collector)
        self.assertIn("' item '", self.collector)
        self.assertIn("def validate_detail_response", self.collector)
        self.assertIn("parse_status", self.collector)
        self.assertIn("fail-on-parse-error-rate", self.collector)
        self.assertNotIn("if not section_summaries:", self.collector)

    def test_workflow_installs_navii_dependencies_and_runs_quality_gate(self) -> None:
        self.assertIn("Install Navii collector dependencies", self.workflow)
        self.assertIn("python3 -m pip install --disable-pip-version-check lxml", self.workflow)
        self.assertIn("collectors/navii_detail/quality_gate.py", self.workflow)
        self.assertIn("quality-status.json", self.workflow)
        self.assertIn("aggregate Navii concurrency must be <= 32", self.workflow)
        self.assertIn('"max_parallel": "16"', self.workflow)
        self.assertIn("fail_on_parse_error_rate:", self.workflow)
        self.assertIn('NAVII_PREFLIGHT_FRACTION: "0.001"', self.workflow)
        self.assertIn('name: "Preflight shard ${{ matrix.shard_index }}"', self.workflow)
        self.assertIn('--sample-fraction "$PREFLIGHT_FRACTION"', self.workflow)
        self.assertIn("--candidate-mode preflight", self.workflow)
        self.assertIn("navii-detail-preflight-audit-${{ github.run_id }}", self.workflow)
        self.assertIn('"full_run_consumes_preflight": False', self.workflow)
        self.assertIn("--fail-fast-on-parse-error", self.workflow)
        self.assertIn("fail-fast: true", self.workflow)
        self.assertNotIn('name: "Preflight canary"', self.workflow)
        preflight_body = re.search(
            r"\n  preflight:\n(?P<body>.*?)\n  preflight-audit:\n",
            self.workflow,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(preflight_body)
        self.assertNotIn("--sample-per-kind 25", preflight_body.group("body"))
        package_body = re.search(
            r"\n  package:\n(?P<body>.*)",
            self.workflow,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(package_body)
        self.assertIn("pattern: navii-detail-shard-*-${{ github.run_id }}", package_body.group("body"))
        self.assertNotIn("pattern: navii-detail-preflight-shard-*-${{ github.run_id }}", package_body.group("body"))
        self.assertIn("concurrent.futures.wait(", self.collector)
        self.assertNotIn("concurrent.futures.as_completed(", self.collector)


if __name__ == "__main__":
    unittest.main()
