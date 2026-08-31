#!/usr/bin/env python3
"""Validate aggregated Navii detail extraction metrics before handoff."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


REQUIRED_EXTRACTION_METRICS = (
    "section_count",
    "table_row_count",
    "link_row_count",
    "phone_number_row_count",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--candidate-mode", choices=("all", "sample"), required=True)
    parser.add_argument("--sample-per-kind", type=int, required=True)
    parser.add_argument("--kinds", required=True)
    parser.add_argument("--fail-on-fetch-error-rate", type=float, required=True)
    return parser.parse_args()


def int_value(payload: dict[str, Any], key: str) -> int:
    try:
        return int(payload.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def aggregate_metrics(payloads: Iterable[dict[str, Any]]) -> tuple[Counter[str], dict[str, Counter[str]], dict[str, set[str]]]:
    totals: Counter[str] = Counter()
    by_kind: dict[str, Counter[str]] = defaultdict(Counter)
    fingerprints: dict[str, set[str]] = defaultdict(set)
    for payload in payloads:
        for key in (
            "candidate_count",
            "fetch_ok_count",
            "fetch_error_count",
            "parse_ok_count",
            "parse_error_count",
            "section_count",
            "table_rows",
            "link_rows",
            "phone_number_rows",
        ):
            totals[key] += int_value(payload, key)
        for kind, values in (payload.get("kind_metrics") or {}).items():
            if not isinstance(values, dict):
                continue
            for key in (
                "candidate_count",
                "fetch_ok_count",
                "fetch_error_count",
                "parse_ok_count",
                "parse_error_count",
                "section_count",
                "table_row_count",
                "link_row_count",
                "phone_number_row_count",
                "unknown_count",
                "fallback_count",
            ):
                by_kind[kind][key] += int_value(values, key)
            for fingerprint in values.get("structure_fingerprints") or []:
                if isinstance(fingerprint, str) and fingerprint:
                    fingerprints[kind].add(fingerprint)
    return totals, by_kind, fingerprints


def evaluate_quality(
    payloads: list[dict[str, Any]],
    *,
    execute: bool,
    candidate_mode: str,
    sample_per_kind: int,
    kinds: list[str],
    fail_on_fetch_error_rate: float,
) -> dict[str, Any]:
    gate = "canary" if candidate_mode == "sample" else "full"
    if not execute:
        return {
            "schema_version": "1.0",
            "quality_status": "not_applicable",
            "gate": gate,
            "failure_reasons": [],
        }

    failures: list[str] = []
    if not payloads:
        failures.append("no executed shard metrics found")
    if any(payload.get("status") != "executed" for payload in payloads):
        failures.append("all shard metrics must have status=executed")

    totals, by_kind, fingerprints = aggregate_metrics(payloads)
    fetch_denominator = totals["fetch_ok_count"] + totals["fetch_error_count"]
    fetch_error_rate = (
        totals["fetch_error_count"] / fetch_denominator * 100
        if fetch_denominator
        else 0.0
    )
    parse_denominator = totals["parse_ok_count"] + totals["parse_error_count"]
    parse_error_rate = (
        totals["parse_error_count"] / parse_denominator * 100
        if parse_denominator
        else 0.0
    )
    if fetch_error_rate > fail_on_fetch_error_rate:
        failures.append(
            f"aggregate fetch error rate {fetch_error_rate:.2f}% exceeded {fail_on_fetch_error_rate:.2f}%"
        )
    if totals["parse_error_count"] > 0:
        failures.append(f"parse errors detected: {totals['parse_error_count']}")
    if candidate_mode == "sample" and sample_per_kind < 25:
        failures.append("canary sample_per_kind must be at least 25")

    per_kind: dict[str, dict[str, Any]] = {}
    for kind in kinds:
        metrics = by_kind.get(kind, Counter())
        kind_failures: list[str] = []
        if candidate_mode == "sample" and metrics["candidate_count"] < sample_per_kind:
            kind_failures.append(
                f"candidate_count {metrics['candidate_count']} below canary minimum {sample_per_kind}"
            )
        if metrics["candidate_count"] <= 0:
            kind_failures.append("candidate_count is zero")
        if metrics["fetch_ok_count"] <= 0:
            kind_failures.append("fetch_ok_count is zero")
        if metrics["parse_error_count"] > 0:
            kind_failures.append(f"parse_error_count={metrics['parse_error_count']}")
        if metrics["unknown_count"] > 0:
            kind_failures.append(f"unknown_count={metrics['unknown_count']}")
        if metrics["fallback_count"] > 0:
            kind_failures.append(f"fallback_count={metrics['fallback_count']}")
        for metric in REQUIRED_EXTRACTION_METRICS:
            if metrics[metric] <= 0:
                kind_failures.append(f"{metric} is zero")
        failures.extend(f"{kind}: {reason}" for reason in kind_failures)
        per_kind[kind] = {
            **{key: int(value) for key, value in metrics.items()},
            "structure_fingerprints": sorted(fingerprints.get(kind, set())),
            "quality_status": "fail" if kind_failures else "pass",
            "failure_reasons": kind_failures,
        }

    return {
        "schema_version": "1.0",
        "quality_status": "fail" if failures else "pass",
        "gate": gate,
        "candidate_mode": candidate_mode,
        "sample_per_kind": sample_per_kind,
        "kinds": kinds,
        "aggregate": {
            **{key: int(value) for key, value in totals.items()},
            "fetch_error_rate_percent": round(fetch_error_rate, 4),
            "parse_error_rate_percent": round(parse_error_rate, 4),
        },
        "per_kind": per_kind,
        "failure_reasons": failures,
    }


def load_metrics(metrics_dir: Path) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for path in sorted(metrics_dir.glob("*run-metrics.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid metrics file: {path.name}: {type(exc).__name__}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"metrics file is not an object: {path.name}")
        payloads.append(payload)
    return payloads


def main() -> int:
    args = parse_args()
    try:
        payloads = load_metrics(args.metrics_dir)
        result = evaluate_quality(
            payloads,
            execute=args.execute,
            candidate_mode=args.candidate_mode,
            sample_per_kind=args.sample_per_kind,
            kinds=[kind.strip() for kind in args.kinds.split(",") if kind.strip()],
            fail_on_fetch_error_rate=args.fail_on_fetch_error_rate,
        )
    except ValueError as exc:
        result = {
            "schema_version": "1.0",
            "quality_status": "fail",
            "gate": "canary" if args.candidate_mode == "sample" else "full",
            "failure_reasons": [str(exc)],
        }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["quality_status"] in {"pass", "not_applicable"} else 2


if __name__ == "__main__":
    sys.exit(main())
