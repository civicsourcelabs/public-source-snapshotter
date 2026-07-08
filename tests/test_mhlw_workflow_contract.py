"""MHLW monthly workflow automation contract tests."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = REPO_ROOT / ".github/workflows/mhlw-monthly-source-snapshot.yml"
COLLECTOR_PATH = REPO_ROOT / "collectors/mhlw_monthly/collect.py"


class MhlwWorkflowContractTest(unittest.TestCase):
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

    def test_schedule_and_dispatch_derive_snapshot_date_and_run_label(self) -> None:
        self.assertIn("def target_month_start() -> str:", self.workflow)
        self.assertIn('return f"{jst_now:%Y-%m}-01"', self.workflow)
        self.assertEqual(self.workflow.count('"source_snapshot_date": target_month_start()'), 2)

        self.assertIn("def run_label_for(resolved: dict[str, str]) -> str:", self.workflow)
        self.assertEqual(self.workflow.count('resolved["run_label"] = run_label_for(resolved)'), 2)
        self.assertIn('return f"collector-mhlw-monthly-{yyyymm}-{suffix}"', self.workflow)

    def test_scheduled_run_stays_full_encrypted_snapshot(self) -> None:
        expected_pairs = {
            '"execute": "true"': "scheduled run must execute",
            '"confirm": "owner-approved-public-source-snapshot"': "scheduled run needs owner gate token",
            '"source_manifest": source_manifest': "scheduled run must use repo manifest",
            '"artifact_mode": "encrypted_full"': "scheduled run must upload encrypted full package",
            '"pipeline_slug": ""': "scheduled run must not scope product slug",
            '"region": ""': "scheduled run must not scope region",
            '"source_type": ""': "scheduled run must not scope source type",
            '"max_sources": "0"': "scheduled run must be full snapshot",
        }
        for snippet, message in expected_pairs.items():
            self.assertIn(snippet, self.workflow, message)

    def test_artifact_name_uses_derived_run_label_and_github_run_id(self) -> None:
        self.assertIn(
            "name: mhlw-monthly-handoff-${{ steps.config.outputs.run_label }}-${{ github.run_id }}",
            self.workflow,
        )

    def test_collector_keeps_manifest_outputs_for_downstream_resolution(self) -> None:
        self.assertIn('write_json(out_dir / "manifest" / "collector-run-manifest.json"', self.collector)
        self.assertIn('write_json(out_dir / "manifest" / "mhlw-source-coverage.json"', self.collector)
        self.assertIn('write_json(out_dir / "manifest" / "mhlw-source-units.json"', self.collector)
        self.assertIn('"source_snapshot_date": source_snapshot_date', self.collector)
        self.assertIn('"run_label": args.run_label', self.collector)


if __name__ == "__main__":
    unittest.main()
