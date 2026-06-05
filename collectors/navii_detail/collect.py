#!/usr/bin/env python3
"""
Public-source Navii detail-page snapshot collector.

This collector reads official Navii open-data ZIP files, builds deterministic
detail-page candidates, and optionally fetches approved source pages. It writes
local shard artifacts only. It does not require or access any private datastore,
external-service secret, deploy provider, privileged role, or production secret.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import html
import json
import os
import random
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable


OPEN_DATA_FILES = {
    "hospital": "01-1_hospital_facility_info_20251201.csv",
    "clinic": "02-1_clinic_facility_info_20251201.csv",
    "dental": "03-1_dental_facility_info_20251201.csv",
    "pharmacy": "05_pharmacy_20251201.csv",
}

NAVII_DETAIL_BASE = (
    "https://www.iryou.teikyouseido.mhlw.go.jp/znk-web/juminkanja/S2430/initialize"
)

PRODUCT_BY_KIND = {
    "hospital": "medical",
    "clinic": "medical",
    "dental": "dental",
    "pharmacy": "pharmacy",
}

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; CivicSourceSnapshotter Navii detail collector; "
    "owner-approved-public-source-snapshot)"
)

TRANSPARENT_USER_AGENT_POOL = (
    "Mozilla/5.0 (compatible; CivicSourceSnapshotter Navii detail collector; owner-approved-public-source-snapshot)",
    "Mozilla/5.0 (compatible; CivicSourceSnapshotter source snapshot worker; owner-approved-public-source-snapshot)",
    "Mozilla/5.0 (compatible; CivicSourceSnapshotter public source research; owner-approved-public-source-snapshot)",
)

TARGET_GROUPS = {
    "personnel": (
        "医療機関の人員配置",
        "医師数・看護師数",
        "医療従事者の専門性",
        "医療従事者の人員数",
        "看護師配置状況",
        "薬剤師数",
        "勤務薬剤師",
        "在籍人数",
        "常勤換算",
        "従事者合計",
    ),
    "home_regional_care": (
        "かかりつけ医機能",
        "在宅医療",
        "地域連携",
        "医療連携",
        "地域医療連携",
        "居宅サービス",
        "訪問看護",
        "訪問診療",
        "往診",
        "在宅患者",
        "退院時共同指導",
        "地域包括",
        "介護関連施設",
    ),
    "appointment_outpatient_hours": (
        "予約診療",
        "予約有無",
        "初診時予約",
        "再診時予約",
        "予約外診察",
        "外来診察対応",
        "診療時間",
        "外来受付時間",
        "休診日",
        "営業日",
        "開店時間",
        "閉店日",
    ),
    "phone_contact": (
        "電話番号",
        "電話による診療予約",
        "予約用電話番号",
        "営業日の開店時間内電話番号",
        "夜間・休日の電話番号",
        "時間外の対応連絡先",
        "時間外対応",
    ),
}

ALL_DETAIL_GROUP = "all_detail"

PHONE_EXCLUDED_LABEL_TERMS = ("FAX", "ＦＡＸ", "ファクシミリ")
PHONE_CONTACT_KIND_TERMS = {
    "after_hours": ("夜間", "休日", "時間外"),
    "appointment": ("予約",),
    "business_hours": ("営業日", "開店時間内"),
}
PHONE_CHAR_TRANSLATION = str.maketrans(
    "０１２３４５６７８９ー－−‐（）。　",
    "0123456789----(). ",
)
PHONE_CANDIDATE_PATTERN = re.compile(r"(?<!\d)0\d[\d()\-\s.]{7,18}\d(?!\d)")


@dataclass(frozen=True)
class NaviiCandidate:
    source_kind: str
    product_slug: str
    navii_id: str
    pref_cd: str
    kikan_kbn: str
    kikan_cd: str
    name: str
    address: str
    detail_url: str


@dataclass(frozen=True)
class SectionTable:
    section_title: str
    table_index: int
    rows: list[list[str]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        "--open-data-dir",
        dest="open_data_dir",
        type=Path,
        help="Directory containing official MHLW Navii open-data ZIP files.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Output directory for collector artifacts.",
    )
    parser.add_argument(
        "--source-id",
        default="navii_detail",
        help="Source id written to run-metrics.json.",
    )
    parser.add_argument(
        "--source-snapshot-date",
        default="2025-12-01",
        help="Official source snapshot date written to run-metrics.json.",
    )
    parser.add_argument(
        "--run-label",
        default="collector-navii-detail-canary",
        help="Artifact run label written to run-metrics.json.",
    )
    parser.add_argument(
        "--artifact-mode",
        choices=("summary_only", "encrypted_full"),
        default="summary_only",
        help="Workflow packaging mode. The collector always writes local raw shard files.",
    )
    parser.add_argument(
        "--sample-per-kind",
        type=int,
        default=3,
        help="Candidate rows to take per Navii open-data kind.",
    )
    parser.add_argument(
        "--sample-strategy",
        choices=("first", "prefecture-stratified"),
        default="first",
        help="How to sample candidates from each Navii open-data kind.",
    )
    parser.add_argument(
        "--max-pages-per-shard",
        "--max-pages",
        dest="max_pages",
        type=int,
        default=400,
        help="Maximum detail pages to fetch after sharding. Use 0 for no limit.",
    )
    parser.add_argument(
        "--kinds",
        default="hospital,clinic,dental,pharmacy",
        help="Comma-separated kinds: hospital,clinic,dental,pharmacy.",
    )
    parser.add_argument(
        "--navii-id",
        action="append",
        default=[],
        help="Specific Navii open-data ID to include. May be passed repeatedly.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Fetch Navii detail HTML for selected candidates. Default is dry-run.",
    )
    parser.add_argument(
        "--all-candidates",
        action="store_true",
        default=True,
        help="Select all candidates for the requested kinds before sharding.",
    )
    parser.add_argument(
        "--sample-candidates",
        dest="all_candidates",
        action="store_false",
        help="Use sample-per-kind selection instead of full candidate selection.",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Total number of deterministic shards.",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="Zero-based shard index to run.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Bounded parallel detail-page fetch workers.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=1.0,
        help="Minimum pause between request submissions in --execute mode.",
    )
    parser.add_argument(
        "--jitter-seconds",
        type=float,
        default=0.0,
        help="Additional random jitter added to each request submission pause.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout for detail page requests.",
    )
    parser.add_argument(
        "--retry-count",
        type=int,
        default=2,
        help="Retry count for transient detail-page fetch failures.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=2.0,
        help="Base retry backoff seconds. Actual backoff grows linearly by attempt.",
    )
    parser.add_argument(
        "--insecure-skip-tls-verify",
        action="store_true",
        help="Disable TLS verification for local troubleshooting only.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP User-Agent used in --execute mode.",
    )
    parser.add_argument(
        "--user-agent-mode",
        choices=("fixed", "rotate-transparent"),
        default="fixed",
        help="Use a fixed transparent User-Agent, or rotate among transparent collector variants.",
    )
    parser.add_argument(
        "--gzip-output",
        dest="gzip_output",
        action="store_true",
        default=True,
        help="Write CSV artifacts as .csv.gz. Recommended for GitHub Actions artifact storage.",
    )
    parser.add_argument(
        "--no-gzip-output",
        dest="gzip_output",
        action="store_false",
        help="Write CSV artifacts without gzip compression.",
    )
    parser.add_argument(
        "--resume-from-existing",
        action="store_true",
        help="Skip candidates already marked fetch_status=ok in existing page coverage output.",
    )
    parser.add_argument(
        "--fail-on-fetch-error-rate",
        type=float,
        default=100.0,
        help="Exit non-zero if fetch error rate is greater than this percent.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Write progress JSON to stderr every N completed candidates. Use 0 to disable.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run parser self-test and exit.",
    )
    return parser.parse_args()


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(strip_tags(value))).strip()


def strip_tags(value: str) -> str:
    value = re.sub(
        r'<div class="tooltipBlock">.*?</div>\s*</div>\s*<span class="iconYougoKaisetsu"></span>',
        "",
        value,
        flags=re.S,
    )
    value = re.sub(r'<div class="yougoKaisetsuText.*?</div>\s*</div>', "", value, flags=re.S)
    value = re.sub(r"<br\s*/?>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    return value


class TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        cleaned = data.strip()
        if cleaned:
            self.parts.append(cleaned)

    def text(self) -> str:
        return re.sub(r"\s+", " ", " ".join(self.parts)).strip()


def html_to_text(fragment: str) -> str:
    parser = TextCollector()
    parser.feed(fragment)
    return parser.text()


def classify_text(text: str) -> list[str]:
    groups: list[str] = []
    for group, terms in TARGET_GROUPS.items():
        if any(term in text for term in terms):
            groups.append(group)
    return groups


def parse_navii_id(navii_id: str) -> tuple[str, str, str]:
    digits = re.sub(r"\D", "", navii_id or "")
    if len(digits) < 4:
        return "", "", ""
    return digits[:2], digits[2:3], digits[3:]


def build_detail_url(pref_cd: str, kikan_kbn: str, kikan_cd: str) -> str:
    query = urllib.parse.urlencode(
        {
            "prefCd": pref_cd,
            "kikanKbn": kikan_kbn,
            "kikanCd": kikan_cd,
        }
    )
    return f"{NAVII_DETAIL_BASE}?{query}"


def open_data_zip_for(open_data_dir: Path, csv_name: str) -> Path:
    for zip_path in sorted(open_data_dir.glob("*.zip")):
        with zipfile.ZipFile(zip_path) as archive:
            if csv_name in archive.namelist():
                return zip_path
    raise FileNotFoundError(f"Could not find {csv_name} in {open_data_dir}")


def read_open_data_rows(open_data_dir: Path, kinds: Iterable[str]) -> list[NaviiCandidate]:
    rows: list[NaviiCandidate] = []
    for kind in kinds:
        csv_name = OPEN_DATA_FILES[kind]
        zip_path = open_data_zip_for(open_data_dir, csv_name)
        with zipfile.ZipFile(zip_path) as archive:
            with archive.open(csv_name) as raw:
                text = raw.read().decode("utf-8-sig")
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            navii_id = (row.get("ID") or "").strip()
            pref_cd, kikan_kbn, kikan_cd = parse_navii_id(navii_id)
            if not (pref_cd and kikan_kbn and kikan_cd):
                continue
            name = (
                row.get("正式名称")
                or row.get("名称")
                or row.get("略称")
                or ""
            ).strip()
            address = (row.get("所在地") or "").strip()
            rows.append(
                NaviiCandidate(
                    source_kind=kind,
                    product_slug=PRODUCT_BY_KIND[kind],
                    navii_id=navii_id,
                    pref_cd=pref_cd,
                    kikan_kbn=kikan_kbn,
                    kikan_cd=kikan_cd,
                    name=name,
                    address=address,
                    detail_url=build_detail_url(pref_cd, kikan_kbn, kikan_cd),
                )
            )
    return rows


def select_candidates(
    rows: list[NaviiCandidate],
    *,
    kinds: list[str],
    sample_per_kind: int,
    sample_strategy: str,
    navii_ids: set[str],
    all_candidates: bool,
) -> list[NaviiCandidate]:
    selected: list[NaviiCandidate] = []
    seen: set[str] = set()

    for row in rows:
        if row.navii_id in navii_ids and row.navii_id not in seen:
            selected.append(row)
            seen.add(row.navii_id)

    if all_candidates:
        for row in rows:
            if row.source_kind not in kinds or row.navii_id in seen:
                continue
            selected.append(row)
            seen.add(row.navii_id)
        return selected

    for kind in kinds:
        kind_rows = [row for row in rows if row.source_kind == kind and row.navii_id not in seen]
        if sample_strategy == "prefecture-stratified":
            sampled = stratified_by_prefecture(kind_rows, sample_per_kind)
        else:
            sampled = kind_rows[:sample_per_kind]

        for row in sampled:
            if row.navii_id in seen:
                continue
            selected.append(row)
            seen.add(row.navii_id)

    return selected


def apply_shard(
    candidates: list[NaviiCandidate],
    *,
    shard_count: int,
    shard_index: int,
) -> list[NaviiCandidate]:
    if shard_count < 1:
        raise SystemExit("--shard-count must be >= 1")
    if shard_index < 0 or shard_index >= shard_count:
        raise SystemExit("--shard-index must be between 0 and shard-count - 1")
    if shard_count == 1:
        return candidates
    return [
        candidate
        for index, candidate in enumerate(candidates)
        if index % shard_count == shard_index
    ]


def stratified_by_prefecture(
    rows: list[NaviiCandidate],
    sample_size: int,
) -> list[NaviiCandidate]:
    if sample_size <= 0:
        return []

    by_pref: dict[str, list[NaviiCandidate]] = defaultdict(list)
    for row in rows:
        by_pref[row.pref_cd].append(row)

    selected: list[NaviiCandidate] = []
    round_index = 0
    pref_codes = sorted(by_pref)
    while len(selected) < sample_size:
        available_prefs = [
            pref_cd for pref_cd in pref_codes if round_index < len(by_pref[pref_cd])
        ]
        if not available_prefs:
            break
        take_count = min(sample_size - len(selected), len(available_prefs))
        for pref_cd in evenly_spaced(available_prefs, take_count):
            selected.append(by_pref[pref_cd][round_index])
        round_index += 1
    return selected


def evenly_spaced(values: list[str], take_count: int) -> list[str]:
    if take_count <= 0:
        return []
    if take_count >= len(values):
        return list(values)
    if take_count == 1:
        return [values[len(values) // 2]]

    max_index = len(values) - 1
    indexes = [round(index * max_index / (take_count - 1)) for index in range(take_count)]
    return [values[index] for index in indexes]


def output_path(path: Path, *, gzip_output: bool) -> Path:
    if not gzip_output:
        return path
    return path.with_name(f"{path.name}.gz")


def temp_output_path(path: Path) -> Path:
    suffix = f".tmp-{os.getpid()}"
    return path.with_name(f"{path.name}{suffix}")


def open_csv_text(path: Path, mode: str):
    if ".gz" in path.name:
        return gzip.open(path, mode, newline="", encoding="utf-8")
    return path.open(mode, newline="", encoding="utf-8")


def iter_csv_rows(path: Path) -> Iterable[dict[str, str]]:
    if not path.exists():
        return
    with open_csv_text(path, "rt") as handle:
        yield from csv.DictReader(handle)


CANDIDATE_FIELDNAMES = [
    "source_kind",
    "product_slug",
    "navii_id",
    "pref_cd",
    "kikan_kbn",
    "kikan_cd",
    "name",
    "address",
    "detail_url",
]

SUMMARY_FIELDNAMES = [
    "source_kind",
    "product_slug",
    "navii_id",
    "pref_cd",
    "kikan_kbn",
    "kikan_cd",
    "name",
    "address",
    "detail_url",
    "fetch_status",
    "target_group",
    "section_title",
    "section_text_sample",
    "table_count",
    "has_extractable_table",
    "error",
]

TABLE_FIELDNAMES = [
    "source_kind",
    "product_slug",
    "navii_id",
    "pref_cd",
    "kikan_kbn",
    "kikan_cd",
    "name",
    "address",
    "detail_url",
    "target_group",
    "section_title",
    "table_index",
    "row_number",
    "row_label",
    "values_joined",
    "cell_count",
    "raw_row_joined",
]

LINK_FIELDNAMES = [
    "source_kind",
    "product_slug",
    "navii_id",
    "pref_cd",
    "kikan_kbn",
    "kikan_cd",
    "name",
    "address",
    "detail_url",
    "target_group",
    "section_title",
    "table_index",
    "row_number",
    "cell_index",
    "row_label",
    "link_text",
    "link_href_raw",
    "link_href_resolved",
    "raw_row_joined",
]

PHONE_FIELDNAMES = [
    "source_kind",
    "product_slug",
    "navii_id",
    "pref_cd",
    "kikan_kbn",
    "kikan_cd",
    "name",
    "address",
    "detail_url",
    "target_group",
    "phone_contact_kind",
    "phone_source_section",
    "phone_source_label",
    "phone_number_raw",
    "phone_number_normalized",
    "raw_row_joined",
]

PAGE_COVERAGE_FIELDNAMES = [
    "source_kind",
    "product_slug",
    "navii_id",
    "pref_cd",
    "kikan_kbn",
    "kikan_cd",
    "name",
    "address",
    "detail_url",
    "fetch_status",
    "target_group",
    "has_target_group",
    "has_extractable_table",
    "section_count",
    "table_count",
    "table_row_count",
    "error",
]

COVERAGE_SUMMARY_FIELDNAMES = [
    "source_kind",
    "product_slug",
    "target_group",
    "candidate_count",
    "fetch_ok_count",
    "fetch_error_count",
    "group_present_count",
    "group_missing_count",
    "extractable_table_count",
    "section_count",
    "table_count",
    "table_row_count",
    "group_present_pct",
    "extractable_table_pct",
]


def csv_writer(path: Path, fieldnames: list[str]):
    handle = open_csv_text(path, "wt")
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    return handle, writer


def write_candidates(path: Path, candidates: list[NaviiCandidate]) -> None:
    with open_csv_text(path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_FIELDNAMES)
        writer.writeheader()
        for row in candidates:
            writer.writerow(row.__dict__)


def fetch_detail_html(
    url: str,
    *,
    user_agent: str,
    user_agent_mode: str,
    request_seed: int,
    timeout_seconds: float,
    insecure_skip_tls_verify: bool,
    retry_count: int,
    retry_backoff_seconds: float,
) -> str:
    context = None
    if insecure_skip_tls_verify:
        context = ssl._create_unverified_context()
    last_error: BaseException | None = None
    for attempt in range(max(retry_count, 0) + 1):
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": choose_user_agent(
                    user_agent=user_agent,
                    user_agent_mode=user_agent_mode,
                    request_seed=request_seed + attempt,
                )
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(charset, errors="replace")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ssl.SSLError) as exc:
            last_error = exc
            if attempt >= retry_count or not is_retryable_fetch_error(exc):
                raise
            time.sleep(retry_backoff_seconds * (attempt + 1))
    raise RuntimeError(f"unreachable fetch retry state: {last_error}")


def choose_user_agent(*, user_agent: str, user_agent_mode: str, request_seed: int) -> str:
    if user_agent_mode == "rotate-transparent":
        return TRANSPARENT_USER_AGENT_POOL[request_seed % len(TRANSPARENT_USER_AGENT_POOL)]
    return user_agent


def is_retryable_fetch_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in {408, 409, 425, 429, 500, 502, 503, 504}
    return True


def iter_section_html(page_html: str) -> Iterable[tuple[str, str]]:
    item_pattern = re.compile(
        r'<div class="item">\s*<h3[^>]*>.*?<div>(?P<title>.*?)</div>.*?</h3>\s*'
        r'<div class="details[^"]*"[^>]*>(?P<body>.*?)</div><!-- /\.\s*details -->',
        flags=re.S,
    )
    for match in item_pattern.finditer(page_html):
        title = normalize_text(match.group("title"))
        body = match.group("body")
        yield title, body


def extract_tables(section_title: str, body_html: str) -> list[SectionTable]:
    tables: list[SectionTable] = []
    for table_index, table_match in enumerate(
        re.finditer(r"<table\b[^>]*>(.*?)</table>", body_html, flags=re.S | re.I),
        start=1,
    ):
        table_html = table_match.group(1)
        rows: list[list[str]] = []
        for tr_match in re.finditer(r"<tr\b[^>]*>(.*?)</tr>", table_html, flags=re.S | re.I):
            tr_html = tr_match.group(1)
            cells = [
                normalize_text(cell_match.group(2))
                for cell_match in re.finditer(
                    r"<(th|td)\b[^>]*>(.*?)</\1>",
                    tr_html,
                    flags=re.S | re.I,
                )
            ]
            cells = [cell for cell in cells if cell]
            if cells:
                rows.append(cells)
        if rows:
            tables.append(SectionTable(section_title=section_title, table_index=table_index, rows=rows))
    return tables


def extract_href(anchor_attrs: str) -> str:
    match = re.search(
        r"""\bhref\s*=\s*(?:"(?P<double>[^"]*)"|'(?P<single>[^']*)'|(?P<bare>[^\s>]+))""",
        anchor_attrs,
        flags=re.I,
    )
    if not match:
        return ""
    return html.unescape(match.group("double") or match.group("single") or match.group("bare") or "")


def extract_links(
    *,
    section_title: str,
    body_html: str,
    page_url: str,
) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for table_index, table_match in enumerate(
        re.finditer(r"<table\b[^>]*>(.*?)</table>", body_html, flags=re.S | re.I),
        start=1,
    ):
        table_html = table_match.group(1)
        for row_number, tr_match in enumerate(
            re.finditer(r"<tr\b[^>]*>(.*?)</tr>", table_html, flags=re.S | re.I),
            start=1,
        ):
            tr_html = tr_match.group(1)
            cell_matches = list(
                re.finditer(r"<(th|td)\b[^>]*>(.*?)</\1>", tr_html, flags=re.S | re.I)
            )
            cells = [normalize_text(cell_match.group(2)) for cell_match in cell_matches]
            row_label = next((cell for cell in cells if cell), "")
            raw_row_joined = " | ".join(cell for cell in cells if cell)
            for cell_index, cell_match in enumerate(cell_matches, start=1):
                cell_html = cell_match.group(2)
                for anchor_match in re.finditer(
                    r"<a\b(?P<attrs>[^>]*)>(?P<body>.*?)</a>",
                    cell_html,
                    flags=re.S | re.I,
                ):
                    href_raw = extract_href(anchor_match.group("attrs"))
                    if not href_raw:
                        continue
                    link_text = normalize_text(anchor_match.group("body"))
                    links.append(
                        {
                            "target_group": ALL_DETAIL_GROUP,
                            "section_title": section_title,
                            "table_index": str(table_index),
                            "row_number": str(row_number),
                            "cell_index": str(cell_index),
                            "row_label": row_label,
                            "link_text": link_text,
                            "link_href_raw": href_raw,
                            "link_href_resolved": urllib.parse.urljoin(page_url, href_raw),
                            "raw_row_joined": raw_row_joined,
                        }
                    )
    return links


def normalized_phone_source(value: str) -> str:
    return re.sub(r"\s+", " ", value.translate(PHONE_CHAR_TRANSLATION)).strip()


def normalize_phone_number(value: str) -> str:
    return re.sub(r"\D", "", normalized_phone_source(value))


def is_valid_phone_number(value: str) -> bool:
    return value.startswith("0") and 9 <= len(value) <= 11


def classify_phone_contact_kind(text: str) -> str:
    for kind, terms in PHONE_CONTACT_KIND_TERMS.items():
        if any(term in text for term in terms):
            return kind
    return "general"


def extract_phone_numbers(value: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    normalized_source = normalized_phone_source(value)
    for match in PHONE_CANDIDATE_PATTERN.finditer(normalized_source):
        raw_phone = match.group(0).strip()
        normalized = normalize_phone_number(raw_phone)
        if not is_valid_phone_number(normalized) or normalized in seen:
            continue
        pairs.append((raw_phone, normalized))
        seen.add(normalized)
    return pairs


def should_extract_phone_from_row(
    *,
    section_title: str,
    row_label: str,
    raw_row_joined: str,
) -> bool:
    if any(term in row_label for term in PHONE_EXCLUDED_LABEL_TERMS):
        return False
    source_text = f"{section_title} {row_label} {raw_row_joined}"
    return any(term in source_text for term in TARGET_GROUPS["phone_contact"])


def extract_phone_rows(
    *,
    section_title: str,
    tables: list[SectionTable],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for table in tables:
        for row in table.rows:
            row_label = row[0] if row else ""
            values = row[1:] if len(row) > 1 else []
            raw_row_joined = " | ".join(row)
            if not should_extract_phone_from_row(
                section_title=section_title,
                row_label=row_label,
                raw_row_joined=raw_row_joined,
            ):
                continue
            phone_pairs = extract_phone_numbers(" | ".join(values) or raw_row_joined)
            for raw_phone, normalized in phone_pairs:
                dedupe_key = (section_title, row_label, normalized)
                if dedupe_key in seen:
                    continue
                rows.append(
                    {
                        "target_group": "phone_contact",
                        "phone_contact_kind": classify_phone_contact_kind(
                            f"{section_title} {row_label}"
                        ),
                        "phone_source_section": section_title,
                        "phone_source_label": row_label,
                        "phone_number_raw": raw_phone,
                        "phone_number_normalized": normalized,
                        "raw_row_joined": raw_row_joined,
                    }
                )
                seen.add(dedupe_key)
    return rows


def analyze_detail(
    page_html: str,
    *,
    page_url: str = "",
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    summary_rows: list[dict[str, str]] = []
    table_rows: list[dict[str, str]] = []
    phone_rows: list[dict[str, str]] = []
    link_rows: list[dict[str, str]] = []

    for section_title, body_html in iter_section_html(page_html):
        section_text = f"{section_title} {html_to_text(body_html)}"
        tables = extract_tables(section_title, body_html)
        link_rows.extend(
            extract_links(section_title=section_title, body_html=body_html, page_url=page_url)
        )
        summary_rows.append(
            {
                "target_group": ALL_DETAIL_GROUP,
                "section_title": section_title,
                "section_text_sample": section_text[:300],
                "table_count": str(len(tables)),
                "has_extractable_table": "true" if tables else "false",
            }
        )

        for table in tables:
            for row_number, row in enumerate(table.rows, start=1):
                row_label = row[0] if row else ""
                values = row[1:] if len(row) > 1 else []
                table_rows.append(
                    {
                        "target_group": ALL_DETAIL_GROUP,
                        "section_title": table.section_title,
                        "table_index": str(table.table_index),
                        "row_number": str(row_number),
                        "row_label": row_label,
                        "values_joined": " | ".join(values),
                        "cell_count": str(len(row)),
                        "raw_row_joined": " | ".join(row),
                    }
                )

        groups = classify_text(section_text)
        if not groups:
            continue

        if "phone_contact" in groups:
            phone_rows.extend(extract_phone_rows(section_title=section_title, tables=tables))

        for group in groups:
            summary_rows.append(
                {
                    "target_group": group,
                    "section_title": section_title,
                    "section_text_sample": section_text[:300],
                    "table_count": str(len(tables)),
                    "has_extractable_table": "true" if tables else "false",
                }
            )

    return summary_rows, table_rows, phone_rows, link_rows


def write_summary(path: Path, rows: list[dict[str, str]]) -> None:
    with open_csv_text(path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_tables(path: Path, rows: list[dict[str, str]]) -> None:
    with open_csv_text(path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=TABLE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_links(path: Path, rows: list[dict[str, str]]) -> None:
    with open_csv_text(path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=LINK_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_phone_numbers(path: Path, rows: list[dict[str, str]]) -> None:
    with open_csv_text(path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=PHONE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_page_coverage(path: Path, rows: list[dict[str, str]]) -> None:
    with open_csv_text(path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=PAGE_COVERAGE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def summarize_coverage(page_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    counters: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)

    for row in page_rows:
        key = (row["source_kind"], row["product_slug"], row["target_group"])
        counters[key]["candidate_count"] += 1
        if row["fetch_status"] == "ok":
            counters[key]["fetch_ok_count"] += 1
        else:
            counters[key]["fetch_error_count"] += 1
        if row["has_target_group"] == "true":
            counters[key]["group_present_count"] += 1
        else:
            counters[key]["group_missing_count"] += 1
        if row["has_extractable_table"] == "true":
            counters[key]["extractable_table_count"] += 1
        counters[key]["section_count"] += int(row["section_count"] or 0)
        counters[key]["table_count"] += int(row["table_count"] or 0)
        counters[key]["table_row_count"] += int(row["table_row_count"] or 0)

    summary_rows: list[dict[str, str]] = []
    for (source_kind, product_slug, target_group), counter in sorted(counters.items()):
        candidate_count = counter["candidate_count"]
        fetch_ok_count = counter["fetch_ok_count"]
        group_present_count = counter["group_present_count"]
        extractable_table_count = counter["extractable_table_count"]
        summary_rows.append(
            {
                "source_kind": source_kind,
                "product_slug": product_slug,
                "target_group": target_group,
                "candidate_count": str(candidate_count),
                "fetch_ok_count": str(fetch_ok_count),
                "fetch_error_count": str(counter["fetch_error_count"]),
                "group_present_count": str(group_present_count),
                "group_missing_count": str(counter["group_missing_count"]),
                "extractable_table_count": str(extractable_table_count),
                "section_count": str(counter["section_count"]),
                "table_count": str(counter["table_count"]),
                "table_row_count": str(counter["table_row_count"]),
                "group_present_pct": percent(group_present_count, fetch_ok_count),
                "extractable_table_pct": percent(extractable_table_count, fetch_ok_count),
            }
        )
    return summary_rows


def write_coverage_summary(path: Path, rows: list[dict[str, str]]) -> None:
    with open_csv_text(path, "wt") as handle:
        writer = csv.DictWriter(handle, fieldnames=COVERAGE_SUMMARY_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def update_coverage_counters(
    counters: dict[tuple[str, str, str], Counter[str]],
    page_rows: Iterable[dict[str, str]],
) -> None:
    for row in page_rows:
        key = (row["source_kind"], row["product_slug"], row["target_group"])
        counters[key]["candidate_count"] += 1
        if row["fetch_status"] == "ok":
            counters[key]["fetch_ok_count"] += 1
        else:
            counters[key]["fetch_error_count"] += 1
        if row["has_target_group"] == "true":
            counters[key]["group_present_count"] += 1
        else:
            counters[key]["group_missing_count"] += 1
        if row["has_extractable_table"] == "true":
            counters[key]["extractable_table_count"] += 1
        counters[key]["section_count"] += int(row["section_count"] or 0)
        counters[key]["table_count"] += int(row["table_count"] or 0)
        counters[key]["table_row_count"] += int(row["table_row_count"] or 0)


def coverage_summary_from_counters(
    counters: dict[tuple[str, str, str], Counter[str]],
) -> list[dict[str, str]]:
    summary_rows: list[dict[str, str]] = []
    for (source_kind, product_slug, target_group), counter in sorted(counters.items()):
        candidate_count = counter["candidate_count"]
        fetch_ok_count = counter["fetch_ok_count"]
        group_present_count = counter["group_present_count"]
        extractable_table_count = counter["extractable_table_count"]
        summary_rows.append(
            {
                "source_kind": source_kind,
                "product_slug": product_slug,
                "target_group": target_group,
                "candidate_count": str(candidate_count),
                "fetch_ok_count": str(fetch_ok_count),
                "fetch_error_count": str(counter["fetch_error_count"]),
                "group_present_count": str(group_present_count),
                "group_missing_count": str(counter["group_missing_count"]),
                "extractable_table_count": str(extractable_table_count),
                "section_count": str(counter["section_count"]),
                "table_count": str(counter["table_count"]),
                "table_row_count": str(counter["table_row_count"]),
                "group_present_pct": percent(group_present_count, fetch_ok_count),
                "extractable_table_pct": percent(extractable_table_count, fetch_ok_count),
            }
        )
    return summary_rows


def percent(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.00"
    return f"{(numerator / denominator) * 100:.2f}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_self_test() -> None:
    sample = """
    <div class="item">
      <h3 class="heading acHeading"><a><div>医療機関の人員配置</div></a></h3>
      <div class="details idx-20" style="display:block;">
        <div class="ptn4DataArea"><table>
          <tr><th>職種</th><th>総数</th><th>常勤</th><th>非常勤</th></tr>
          <tr><th>医師</th><td>10.1</td><td>5</td><td>5.1</td></tr>
          <tr><th>看護師</th><td>62.04</td><td>61</td><td>1.04</td></tr>
        </table></div>
      </div><!-- /.details -->
    </div>
    <div class="item">
      <h3 class="heading acHeading"><a><div>予約診療の有無</div></a></h3>
      <div class="details idx-4" style="display:block;">
        <div class="ptn1DataArea"><table>
          <tr><th>電話による診療予約の可否</th><td>可能</td></tr>
          <tr><th>予約用電話番号</th><td>０１１−２２２−３３３３</td></tr>
          <tr><th>夜間・休日の電話番号</th><td>011-222-4444</td></tr>
          <tr><th>営業日の開店時間内ファクシミリ番号</th><td>011-222-5555</td></tr>
        </table></div>
      </div><!-- /.details -->
    </div>
    <div class="item">
      <h3 class="heading acHeading"><a><div>電話番号・FAX番号</div></a></h3>
      <div class="details idx-5" style="display:block;">
        <div class="ptn1DataArea"><table>
          <tr><th>連絡先</th><td>011-222-6666</td></tr>
          <tr><th>ファクシミリ番号</th><td>011-222-7777</td></tr>
        </table></div>
      </div><!-- /.details -->
    </div>
    <div class="item">
      <h3 class="heading acHeading"><a><div>案内用ホームページアドレス</div></a></h3>
      <div class="details idx-6" style="display:block;">
        <div class="ptn1DataArea"><table>
          <tr><th>案内用ホームページアドレス</th><td><a href="/clinic/">https://example.test/clinic</a></td></tr>
        </table></div>
      </div><!-- /.details -->
    </div>
    """
    summary, tables, phone_rows, link_rows = analyze_detail(sample, page_url="https://example.test/base/")
    groups = {row["target_group"] for row in summary}
    labels = {row["row_label"] for row in tables}
    normalized_phone_numbers = {row["phone_number_normalized"] for row in phone_rows}
    table_groups = {row["target_group"] for row in tables}
    coverage_rows = build_page_coverage_rows(
        NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="0120116711805",
            pref_cd="01",
            kikan_kbn="2",
            kikan_cd="0116711805",
            name="self test clinic",
            address="北海道",
            detail_url="https://example.test",
        ),
        "ok",
        "",
        summary,
        tables,
    )
    coverage_summary = summarize_coverage(coverage_rows)
    assert "personnel" in groups, groups
    assert "appointment_outpatient_hours" in groups, groups
    assert "phone_contact" in groups, groups
    assert ALL_DETAIL_GROUP in groups, groups
    assert table_groups == {ALL_DETAIL_GROUP}, table_groups
    assert "医師" in labels, labels
    assert "電話による診療予約の可否" in labels, labels
    assert "0112223333" in normalized_phone_numbers, normalized_phone_numbers
    assert "0112224444" in normalized_phone_numbers, normalized_phone_numbers
    assert "0112226666" in normalized_phone_numbers, normalized_phone_numbers
    assert "0112225555" not in normalized_phone_numbers, normalized_phone_numbers
    assert "0112227777" not in normalized_phone_numbers, normalized_phone_numbers
    assert any(
        row["link_href_resolved"] == "https://example.test/clinic/"
        for row in link_rows
    ), link_rows
    assert any(
        row["target_group"] == "personnel" and row["group_present_pct"] == "100.00"
        for row in coverage_summary
    ), coverage_summary
    assert any(
        row["target_group"] == "personnel" and int(row["table_row_count"]) > 0
        for row in coverage_summary
    ), coverage_summary
    print("navii_detail collector self-test passed")


def build_page_coverage_rows(
    candidate: NaviiCandidate,
    fetch_status: str,
    error: str,
    section_summaries: list[dict[str, str]],
    section_tables: list[dict[str, str]],
) -> list[dict[str, str]]:
    section_counts = Counter(row["target_group"] for row in section_summaries)
    extractable_counts = Counter(
        row["target_group"]
        for row in section_summaries
        if row["has_extractable_table"] == "true"
    )
    table_counts: Counter[str] = Counter()
    table_row_counts = Counter(row["target_group"] for row in section_tables)
    table_rows_by_section = Counter(
        row["section_title"]
        for row in section_tables
        if row["target_group"] == ALL_DETAIL_GROUP
    )
    for row in section_summaries:
        target_group = row["target_group"]
        table_counts[target_group] += int(row["table_count"] or 0)
        if target_group != ALL_DETAIL_GROUP:
            table_row_counts[target_group] += table_rows_by_section[row["section_title"]]

    rows: list[dict[str, str]] = []
    for target_group in (ALL_DETAIL_GROUP, *TARGET_GROUPS):
        section_count = section_counts[target_group]
        table_count = table_counts[target_group]
        table_row_count = table_row_counts[target_group]
        rows.append(
            {
                **candidate.__dict__,
                "fetch_status": fetch_status,
                "target_group": target_group,
                "has_target_group": "true" if section_count else "false",
                "has_extractable_table": "true" if extractable_counts[target_group] else "false",
                "section_count": str(section_count),
                "table_count": str(table_count),
                "table_row_count": str(table_row_count),
                "error": error,
            }
        )
    return rows


def process_candidate(
    *,
    index: int,
    candidate: NaviiCandidate,
    args: argparse.Namespace,
) -> dict[str, object]:
    try:
        page_html = fetch_detail_html(
            candidate.detail_url,
            user_agent=args.user_agent,
            user_agent_mode=args.user_agent_mode,
            request_seed=index,
            timeout_seconds=args.timeout_seconds,
            insecure_skip_tls_verify=args.insecure_skip_tls_verify,
            retry_count=args.retry_count,
            retry_backoff_seconds=args.retry_backoff_seconds,
        )
        section_summaries, section_tables, phone_numbers, links = analyze_detail(
            page_html,
            page_url=candidate.detail_url,
        )
        summary_rows: list[dict[str, str]] = []
        table_rows: list[dict[str, str]] = []
        phone_rows: list[dict[str, str]] = []
        link_rows: list[dict[str, str]] = []
        if not section_summaries:
            summary_rows.append(
                {
                    **candidate.__dict__,
                    "fetch_status": "ok",
                    "target_group": "",
                    "section_title": "",
                    "section_text_sample": "",
                    "table_count": "0",
                    "has_extractable_table": "false",
                    "error": "no target sections found",
                }
            )
        for row in section_summaries:
            summary_rows.append({**candidate.__dict__, "fetch_status": "ok", "error": "", **row})
        for row in section_tables:
            table_rows.append({**candidate.__dict__, **row})
        for row in phone_numbers:
            phone_rows.append({**candidate.__dict__, **row})
        for row in links:
            link_rows.append({**candidate.__dict__, **row})
        page_coverage_rows = build_page_coverage_rows(
            candidate, "ok", "", section_summaries, section_tables
        )
        return {
            "index": index,
            "candidate": candidate,
            "fetch_status": "ok",
            "summary_rows": summary_rows,
            "table_rows": table_rows,
            "phone_rows": phone_rows,
            "link_rows": link_rows,
            "page_coverage_rows": page_coverage_rows,
        }
    except (urllib.error.URLError, TimeoutError, ssl.SSLError) as exc:
        error = str(exc)
        summary_rows = [
            {
                **candidate.__dict__,
                "fetch_status": "error",
                "target_group": "",
                "section_title": "",
                "section_text_sample": "",
                "table_count": "0",
                "has_extractable_table": "false",
                "error": error,
            }
        ]
        page_coverage_rows = build_page_coverage_rows(candidate, "error", error, [], [])
        return {
            "index": index,
            "candidate": candidate,
            "fetch_status": "error",
            "summary_rows": summary_rows,
            "table_rows": [],
            "phone_rows": [],
            "link_rows": [],
            "page_coverage_rows": page_coverage_rows,
        }


def ensure_required_args(args: argparse.Namespace) -> None:
    if args.self_test:
        return
    missing = []
    if args.open_data_dir is None:
        missing.append("--open-data-dir")
    if args.out_dir is None:
        missing.append("--out-dir")
    if missing:
        raise SystemExit(f"Missing required arguments: {', '.join(missing)}")


def completed_navii_ids_from_page_coverage(path: Path) -> set[str]:
    completed: dict[str, set[str]] = defaultdict(set)
    for row in iter_csv_rows(path):
        if row.get("fetch_status") == "ok":
            completed[row.get("navii_id", "")].add(row.get("target_group", ""))
    expected_groups = set(TARGET_GROUPS)
    return {
        navii_id
        for navii_id, groups in completed.items()
        if navii_id and expected_groups.issubset(groups)
    }


def copy_existing_rows(
    *,
    source_path: Path,
    writer: csv.DictWriter,
    completed_ids: set[str],
    coverage_counters: dict[tuple[str, str, str], Counter[str]] | None = None,
) -> int:
    copied = 0
    for row in iter_csv_rows(source_path):
        if row.get("navii_id") not in completed_ids:
            continue
        writer.writerow(row)
        copied += 1
        if coverage_counters is not None:
            update_coverage_counters(coverage_counters, [row])
    return copied


def replace_temp_outputs(temp_to_final: dict[Path, Path]) -> None:
    for temp_path, final_path in temp_to_final.items():
        temp_path.replace(final_path)


def main() -> int:
    args = parse_args()
    ensure_required_args(args)
    started_at = utc_now_iso()

    if args.self_test:
        run_self_test()
        return 0

    kinds = [kind.strip() for kind in args.kinds.split(",") if kind.strip()]
    invalid = sorted(set(kinds) - set(OPEN_DATA_FILES))
    if invalid:
        raise SystemExit(f"Unknown kind(s): {', '.join(invalid)}")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    all_rows = read_open_data_rows(args.open_data_dir, kinds)
    selected_candidates = select_candidates(
        all_rows,
        kinds=kinds,
        sample_per_kind=args.sample_per_kind,
        sample_strategy=args.sample_strategy,
        navii_ids=set(args.navii_id),
        all_candidates=args.all_candidates,
    )
    shard_candidates = apply_shard(
        selected_candidates,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
    )
    candidates = shard_candidates[: args.max_pages] if args.max_pages > 0 else shard_candidates

    candidates_path = output_path(args.out_dir / "candidates.csv", gzip_output=args.gzip_output)
    summary_path = output_path(args.out_dir / "summary.csv", gzip_output=args.gzip_output)
    tables_path = output_path(args.out_dir / "table-rows.csv", gzip_output=args.gzip_output)
    links_path = output_path(args.out_dir / "links.csv", gzip_output=args.gzip_output)
    phone_numbers_path = output_path(
        args.out_dir / "phone-numbers.csv",
        gzip_output=args.gzip_output,
    )
    page_coverage_path = output_path(args.out_dir / "page-coverage.csv", gzip_output=args.gzip_output)
    coverage_summary_path = output_path(
        args.out_dir / "coverage-summary.csv",
        gzip_output=args.gzip_output,
    )
    metrics_path = args.out_dir / "run-metrics.json"
    write_candidates(candidates_path, candidates)

    if not args.execute:
        write_summary(summary_path, [])
        write_tables(tables_path, [])
        write_links(links_path, [])
        write_phone_numbers(phone_numbers_path, [])
        write_page_coverage(page_coverage_path, [])
        write_coverage_summary(coverage_summary_path, [])
        completed_at = utc_now_iso()
        metrics = {
            "schema_version": "1.0",
            "status": "dry_run",
            "source_id": args.source_id,
            "source_snapshot_date": args.source_snapshot_date,
            "run_label": args.run_label,
            "artifact_mode": args.artifact_mode,
            "selected_candidate_count": len(selected_candidates),
            "shard_candidate_count": len(shard_candidates),
            "candidate_count": len(candidates),
            "shard_index": args.shard_index,
            "shard_count": args.shard_count,
            "workers": args.workers,
            "fetch_ok_count": 0,
            "fetch_error_count": 0,
            "fetch_error_rate": 0,
            "fetch_error_rate_percent": 0,
            "phone_number_rows": 0,
            "link_rows": 0,
            "started_at": started_at,
            "completed_at": completed_at,
            "candidates": str(candidates_path),
            "summary": str(summary_path),
            "tables": str(tables_path),
            "links": str(links_path),
            "phone_numbers": str(phone_numbers_path),
            "page_coverage": str(page_coverage_path),
            "coverage_summary": str(coverage_summary_path),
            "metrics": str(metrics_path),
        }
        metrics_path.write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(
            {
                "status": "dry_run",
                "selected_candidate_count": len(selected_candidates),
                "shard_candidate_count": len(shard_candidates),
                "candidate_count": len(candidates),
                "shard_index": args.shard_index,
                "shard_count": args.shard_count,
                "candidates": str(candidates_path),
                "links": str(links_path),
                "phone_numbers": str(phone_numbers_path),
                "metrics": str(metrics_path),
                "note": "No Navii detail HTML was fetched. Re-run with --execute after owner approval.",
            }
        )
        return 0

    completed_ids: set[str] = set()
    if args.resume_from_existing:
        completed_ids = completed_navii_ids_from_page_coverage(page_coverage_path) & {
            candidate.navii_id for candidate in candidates
        }
    pending_candidates = [
        candidate for candidate in candidates if candidate.navii_id not in completed_ids
    ]

    output_targets = {
        summary_path: SUMMARY_FIELDNAMES,
        tables_path: TABLE_FIELDNAMES,
        links_path: LINK_FIELDNAMES,
        phone_numbers_path: PHONE_FIELDNAMES,
        page_coverage_path: PAGE_COVERAGE_FIELDNAMES,
    }
    write_paths = output_targets
    temp_to_final: dict[Path, Path] = {}
    if args.resume_from_existing and completed_ids:
        write_paths = {}
        for final_path, fieldnames in output_targets.items():
            temp_path = temp_output_path(final_path)
            temp_to_final[temp_path] = final_path
            write_paths[temp_path] = fieldnames

    coverage_counters: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    counts = Counter()

    summary_handle, summary_writer = csv_writer(
        next(path for path, fields in write_paths.items() if fields is SUMMARY_FIELDNAMES),
        SUMMARY_FIELDNAMES,
    )
    tables_handle, tables_writer = csv_writer(
        next(path for path, fields in write_paths.items() if fields is TABLE_FIELDNAMES),
        TABLE_FIELDNAMES,
    )
    phone_handle, phone_writer = csv_writer(
        next(path for path, fields in write_paths.items() if fields is PHONE_FIELDNAMES),
        PHONE_FIELDNAMES,
    )
    links_handle, links_writer = csv_writer(
        next(path for path, fields in write_paths.items() if fields is LINK_FIELDNAMES),
        LINK_FIELDNAMES,
    )
    page_handle, page_writer = csv_writer(
        next(path for path, fields in write_paths.items() if fields is PAGE_COVERAGE_FIELDNAMES),
        PAGE_COVERAGE_FIELDNAMES,
    )

    try:
        if args.resume_from_existing and completed_ids:
            counts["summary_rows"] += copy_existing_rows(
                source_path=summary_path,
                writer=summary_writer,
                completed_ids=completed_ids,
            )
            counts["table_rows"] += copy_existing_rows(
                source_path=tables_path,
                writer=tables_writer,
                completed_ids=completed_ids,
            )
            counts["link_rows"] += copy_existing_rows(
                source_path=links_path,
                writer=links_writer,
                completed_ids=completed_ids,
            )
            counts["phone_number_rows"] += copy_existing_rows(
                source_path=phone_numbers_path,
                writer=phone_writer,
                completed_ids=completed_ids,
            )
            counts["page_coverage_rows"] += copy_existing_rows(
                source_path=page_coverage_path,
                writer=page_writer,
                completed_ids=completed_ids,
                coverage_counters=coverage_counters,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(args.workers, 1)) as executor:
            future_to_candidate: dict[concurrent.futures.Future[dict[str, object]], NaviiCandidate] = {}
            for index, candidate in enumerate(pending_candidates, start=1):
                future = executor.submit(process_candidate, index=index, candidate=candidate, args=args)
                future_to_candidate[future] = candidate
                if index < len(pending_candidates):
                    delay = args.pause_seconds
                    if args.jitter_seconds > 0:
                        delay += random.uniform(0, args.jitter_seconds)
                    if delay > 0:
                        time.sleep(delay)

            for completed_count, future in enumerate(
                concurrent.futures.as_completed(future_to_candidate),
                start=1,
            ):
                result = future.result()
                summary_rows = result["summary_rows"]
                table_rows = result["table_rows"]
                phone_rows = result["phone_rows"]
                link_rows = result["link_rows"]
                page_coverage_rows = result["page_coverage_rows"]
                assert isinstance(summary_rows, list)
                assert isinstance(table_rows, list)
                assert isinstance(phone_rows, list)
                assert isinstance(link_rows, list)
                assert isinstance(page_coverage_rows, list)

                summary_writer.writerows(summary_rows)
                tables_writer.writerows(table_rows)
                links_writer.writerows(link_rows)
                phone_writer.writerows(phone_rows)
                page_writer.writerows(page_coverage_rows)
                update_coverage_counters(coverage_counters, page_coverage_rows)

                counts["summary_rows"] += len(summary_rows)
                counts["table_rows"] += len(table_rows)
                counts["link_rows"] += len(link_rows)
                counts["phone_number_rows"] += len(phone_rows)
                counts["page_coverage_rows"] += len(page_coverage_rows)
                counts[f"fetch_{result['fetch_status']}"] += 1

                if args.progress_every > 0 and completed_count % args.progress_every == 0:
                    print(
                        json.dumps(
                            {
                                "progress": completed_count,
                                "pending_candidate_count": len(pending_candidates),
                                "shard_index": args.shard_index,
                                "fetch_ok": counts["fetch_ok"],
                                "fetch_error": counts["fetch_error"],
                            },
                            ensure_ascii=False,
                        ),
                        file=sys.stderr,
                    )
    finally:
        summary_handle.close()
        tables_handle.close()
        links_handle.close()
        phone_handle.close()
        page_handle.close()

    coverage_summary_rows = coverage_summary_from_counters(coverage_counters)
    coverage_summary_write_path = coverage_summary_path
    if temp_to_final:
        coverage_summary_write_path = temp_output_path(coverage_summary_path)
        temp_to_final[coverage_summary_write_path] = coverage_summary_path
    write_coverage_summary(coverage_summary_write_path, coverage_summary_rows)
    if temp_to_final:
        replace_temp_outputs(temp_to_final)

    total_fetch_count = counts["fetch_ok"] + counts["fetch_error"]
    fetch_error_rate = (counts["fetch_error"] / total_fetch_count) if total_fetch_count else 0.0
    fetch_error_rate_percent = fetch_error_rate * 100
    completed_at = utc_now_iso()
    metrics = {
        "schema_version": "1.0",
        "status": "executed",
        "source_id": args.source_id,
        "source_snapshot_date": args.source_snapshot_date,
        "run_label": args.run_label,
        "artifact_mode": args.artifact_mode,
        "selected_candidate_count": len(selected_candidates),
        "shard_candidate_count": len(shard_candidates),
        "candidate_count": len(candidates),
        "pending_candidate_count": len(pending_candidates),
        "resumed_candidate_count": len(completed_ids),
        "output_candidate_count": len(completed_ids) + total_fetch_count,
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "workers": args.workers,
        "pause_seconds": args.pause_seconds,
        "jitter_seconds": args.jitter_seconds,
        "retry_count": args.retry_count,
        "fetch_ok_count": counts["fetch_ok"],
        "fetch_error_count": counts["fetch_error"],
        "fetch_error_rate": round(fetch_error_rate, 6),
        "fetch_error_rate_percent": round(fetch_error_rate_percent, 4),
        "started_at": started_at,
        "completed_at": completed_at,
        "summary_rows": counts["summary_rows"],
        "table_rows": counts["table_rows"],
        "link_rows": counts["link_rows"],
        "phone_number_rows": counts["phone_number_rows"],
        "page_coverage_rows": counts["page_coverage_rows"],
        "candidates": str(candidates_path),
        "summary": str(summary_path),
        "tables": str(tables_path),
        "links": str(links_path),
        "phone_numbers": str(phone_numbers_path),
        "page_coverage": str(page_coverage_path),
        "coverage_summary": str(coverage_summary_path),
        "metrics": str(metrics_path),
    }
    metrics_path.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(metrics)
    if fetch_error_rate_percent > args.fail_on_fetch_error_rate:
        print(
            f"Fetch error rate {fetch_error_rate_percent:.2f}% exceeded threshold {args.fail_on_fetch_error_rate:.2f}%",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
