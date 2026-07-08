#!/usr/bin/env python3
"""Resolve the official Navii open-data snapshot for collector runs."""

from __future__ import annotations

import argparse
import html
import json
import re
import ssl
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; CivicSourceSnapshotter Navii source resolver; "
    "owner-approved-public-source-snapshot)"
)
FULLWIDTH_TRANSLATION = str.maketrans("０１２３４５６７８９（）　", "0123456789() ")
SNAPSHOT_HEADING_RE = re.compile(
    r"(?P<year>[0-9０-９]{4})\s*年\s*"
    r"(?P<month>[0-9０-９]{1,2})\s*月\s*"
    r"(?P<day>[0-9０-９]{1,2})\s*日\s*時点"
)


@dataclass(frozen=True)
class SourceSpec:
    kind: str
    facility_kind: str
    expected_filename: str
    normalized_label: str


SOURCE_SPECS = (
    SourceSpec("hospital_facility", "hospital", "hospital_facility.zip", "病院(施設票)"),
    SourceSpec(
        "hospital_speciality_hours",
        "hospital",
        "hospital_speciality_hours.zip",
        "病院(診療科・診療時間票)",
    ),
    SourceSpec("clinic_facility", "clinic", "clinic_facility.zip", "診療所(施設票)"),
    SourceSpec(
        "clinic_speciality_hours",
        "clinic",
        "clinic_speciality_hours.zip",
        "診療所(診療科・診療時間票)",
    ),
    SourceSpec("dental_facility", "dental", "dental_facility.zip", "歯科診療所(施設票)"),
    SourceSpec(
        "dental_speciality_hours",
        "dental",
        "dental_speciality_hours.zip",
        "歯科診療所(診療科・診療時間票)",
    ),
    SourceSpec("pharmacy", "pharmacy", "pharmacy.zip", "薬局"),
)


class OpenDataPageParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.current_snapshot_date = ""
        self.links_by_snapshot: dict[str, dict[str, str]] = {}
        self._heading_tag = ""
        self._heading_parts: list[str] = []
        self._link_href = ""
        self._link_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"h2", "h3", "h4"}:
            self._heading_tag = tag
            self._heading_parts = []
            return
        if tag == "a" and self.current_snapshot_date:
            href = dict(attrs).get("href") or ""
            self._link_href = href
            self._link_parts = []

    def handle_data(self, data: str) -> None:
        if self._heading_tag:
            self._heading_parts.append(data)
        if self._link_href:
            self._link_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._heading_tag and tag == self._heading_tag:
            heading = "".join(self._heading_parts)
            snapshot_date = parse_snapshot_heading(heading)
            if snapshot_date:
                self.current_snapshot_date = snapshot_date
                self.links_by_snapshot.setdefault(snapshot_date, {})
            self._heading_tag = ""
            self._heading_parts = []
            return
        if tag == "a" and self._link_href:
            label = normalize_label("".join(self._link_parts))
            if label:
                resolved_url = urllib.parse.urljoin(self.base_url, html.unescape(self._link_href))
                self.links_by_snapshot.setdefault(self.current_snapshot_date, {})[label] = resolved_url
            self._link_href = ""
            self._link_parts = []


def normalize_label(value: str) -> str:
    translated = value.translate(FULLWIDTH_TRANSLATION)
    return re.sub(r"\s+", "", translated)


def parse_snapshot_heading(value: str) -> str:
    match = SNAPSHOT_HEADING_RE.search(value)
    if not match:
        return ""
    year = int(match.group("year").translate(FULLWIDTH_TRANSLATION))
    month = int(match.group("month").translate(FULLWIDTH_TRANSLATION))
    day = int(match.group("day").translate(FULLWIDTH_TRANSLATION))
    return f"{year:04d}-{month:02d}-{day:02d}"


def expected_monthly_snapshot_date(today: date | None = None) -> str:
    today = today or datetime.now(ZoneInfo("Asia/Tokyo")).date()
    return date(today.year, today.month, 1).isoformat()


def load_html(
    source_url: str,
    *,
    user_agent: str = DEFAULT_USER_AGENT,
    insecure_skip_tls_verify: bool = False,
) -> str:
    request = urllib.request.Request(source_url, headers={"User-Agent": user_agent})
    context = ssl._create_unverified_context() if insecure_skip_tls_verify else None
    with urllib.request.urlopen(request, timeout=30, context=context) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_open_data_links(source_url: str, page_html: str) -> dict[str, dict[str, str]]:
    parser = OpenDataPageParser(source_url)
    parser.feed(page_html)
    return parser.links_by_snapshot


def resolve_manifest(
    manifest: dict,
    *,
    page_html: str,
    expected_snapshot_date: str,
) -> dict:
    source_terms_url = str(manifest.get("source_terms_url") or "")
    if not source_terms_url:
        raise ValueError("source_terms_url is required")
    links_by_snapshot = parse_open_data_links(source_terms_url, page_html)
    available_dates = sorted(links_by_snapshot, reverse=True)
    if expected_snapshot_date not in links_by_snapshot:
        available = ", ".join(available_dates) or "none"
        raise ValueError(
            f"expected source snapshot {expected_snapshot_date} was not found; "
            f"available snapshots: {available}"
        )

    compact_date = expected_snapshot_date.replace("-", "")
    links = links_by_snapshot[expected_snapshot_date]
    source_urls: list[dict[str, str]] = []
    missing: list[str] = []
    date_mismatches: list[str] = []
    for spec in SOURCE_SPECS:
        url = links.get(spec.normalized_label)
        if not url:
            missing.append(spec.normalized_label)
            continue
        if compact_date not in url:
            date_mismatches.append(f"{spec.normalized_label}: {url}")
        source_urls.append(
            {
                "kind": spec.kind,
                "facility_kind": spec.facility_kind,
                "url": url,
                "expected_filename": spec.expected_filename,
                "official_link_text": spec.normalized_label,
            }
        )
    if missing:
        raise ValueError(f"missing source links for {expected_snapshot_date}: {', '.join(missing)}")
    if date_mismatches:
        raise ValueError(
            f"source link date did not match {expected_snapshot_date}: "
            + "; ".join(date_mismatches)
        )

    resolved = dict(manifest)
    resolved["source_snapshot_date"] = expected_snapshot_date
    resolved["source_urls"] = source_urls
    resolved["resolved_at"] = datetime.now(timezone.utc).isoformat()
    resolved["resolution"] = {
        "mode": "official_page",
        "expected_snapshot_date": expected_snapshot_date,
        "available_snapshot_dates": available_dates,
        "source_url_count": len(source_urls),
    }
    return resolved


def run_label_for(snapshot_date: str, suffix: str) -> str:
    if suffix not in {"canary", "full", "scope"}:
        raise ValueError(f"invalid run label suffix: {suffix}")
    return f"collector-navii-detail-{snapshot_date.replace('-', '')}-{suffix}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--html-file", type=Path)
    parser.add_argument("--expected-source-snapshot-date", default="")
    parser.add_argument("--today", default="")
    parser.add_argument("--run-label-suffix", choices=("canary", "full", "scope"), default="canary")
    parser.add_argument("--summary-json", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        run_self_test()
        return 0
    if not args.manifest or not args.out:
        raise SystemExit("--manifest and --out are required unless --self-test is used")

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    today = date.fromisoformat(args.today) if args.today else None
    expected_snapshot_date = (
        args.expected_source_snapshot_date or expected_monthly_snapshot_date(today)
    )
    page_html = (
        args.html_file.read_text(encoding="utf-8")
        if args.html_file
        else load_html(
            str(manifest.get("source_terms_url") or ""),
            insecure_skip_tls_verify=args.insecure_skip_tls_verify,
        )
    )
    resolved = resolve_manifest(
        manifest,
        page_html=page_html,
        expected_snapshot_date=expected_snapshot_date,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(resolved, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary = {
        "status": "ready",
        "source_id": resolved.get("source_id"),
        "source_snapshot_date": expected_snapshot_date,
        "run_label": run_label_for(expected_snapshot_date, args.run_label_suffix),
        "source_manifest": str(args.out),
        "source_url_count": len(resolved["source_urls"]),
    }
    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def run_self_test() -> None:
    html_fixture = """
    <h3>2026年６月１日時点</h3>
    <a href="/content/11121000/01-1_hospital_facility_info_20260601.zip">病院 (施設票)</a>
    <a href="/content/11121000/01-2_hospital_speciality_hours_20260601.zip">病院 (診療科・診療時間票）</a>
    <a href="/content/11121000/02-1_clinic_facility_info_20260601.zip">診療所 (施設票)</a>
    <a href="/content/11121000/02-2_clinic_speciality_hours_20260601.zip">診療所 (診療科・診療時間票）</a>
    <a href="/content/11121000/03-1_dental_facility_info_20260601.zip">歯科診療所 (施設票)</a>
    <a href="/content/11121000/03-2_dental_speciality_hours_20260601.zip">歯科診療所 (診療科・診療時間票）</a>
    <a href="/content/11121000/05_pharmacy_20260601.zip">薬局</a>
    <h3>2026年８月１日時点</h3>
    <a href="/content/11121000/01-1_hospital_facility_info_20260801.zip">病院 (施設票)</a>
    <a href="/content/11121000/01-2_hospital_speciality_hours_20260801.zip">病院 (診療科・診療時間票）</a>
    <a href="/content/11121000/02-1_clinic_facility_info_20260801.zip">診療所 (施設票)</a>
    <a href="/content/11121000/02-2_clinic_speciality_hours_20260801.zip">診療所 (診療科・診療時間票）</a>
    <a href="/content/11121000/03-1_dental_facility_info_20260801.zip">歯科診療所 (施設票)</a>
    <a href="/content/11121000/03-2_dental_speciality_hours_20260801.zip">歯科診療所 (診療科・診療時間票）</a>
    <a href="/content/11121000/05_pharmacy_20260801.zip">薬局</a>
    """
    manifest = {
        "schema_version": "1.0",
        "source_id": "navii_detail",
        "source_name": "medical-information-net-open-data",
        "source_terms_url": "https://www.mhlw.go.jp/stf/example.html",
        "source_urls": [],
    }
    resolved = resolve_manifest(
        manifest,
        page_html=html_fixture,
        expected_snapshot_date="2026-06-01",
    )
    assert resolved["source_snapshot_date"] == "2026-06-01"
    assert len(resolved["source_urls"]) == len(SOURCE_SPECS)
    assert run_label_for("2026-08-01", "full") == "collector-navii-detail-20260801-full"
    assert expected_monthly_snapshot_date(date(2026, 7, 5)) == "2026-07-01"
    assert expected_monthly_snapshot_date(date(2026, 8, 5)) == "2026-08-01"
    assert expected_monthly_snapshot_date(date(2027, 1, 5)) == "2027-01-01"
    try:
        resolve_manifest(manifest, page_html=html_fixture, expected_snapshot_date="2026-07-01")
    except ValueError as exc:
        assert "expected source snapshot 2026-07-01" in str(exc)
    else:
        raise AssertionError("missing expected snapshot must fail closed")


if __name__ == "__main__":
    sys.exit(main())
