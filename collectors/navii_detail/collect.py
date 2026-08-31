#!/usr/bin/env python3
"""
Public-source Navii detail-page snapshot collector.

This collector reads official Navii open-data ZIP files, builds deterministic
detail-page candidates, and optionally fetches approved source pages. It writes
local shard artifacts only. It does not require or access any external datastore,
external-service secret, deploy provider, privileged role, or production secret.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import html
import json
import math
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
from typing import Any, Iterable


OPEN_DATA_FILE_TEMPLATES = {
    "hospital": "01-1_hospital_facility_info_{yyyymmdd}.csv",
    "clinic": "02-1_clinic_facility_info_{yyyymmdd}.csv",
    "dental": "03-1_dental_facility_info_{yyyymmdd}.csv",
    "pharmacy": "05_pharmacy_{yyyymmdd}.csv",
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

NAVII_PARSER_VERSION = "dom-v2"

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
class DetailFetchResponse:
    html: str
    status_code: int
    content_type: str
    final_url: str


class NaviiParseError(ValueError):
    """Raised when a fetched response cannot satisfy the Navii detail contract."""


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
        default="",
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
        "--sample-fraction",
        type=float,
        default=0.0,
        help="Approximate fraction to take per kind when using sample mode. Zero keeps sample-per-kind behavior.",
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
        help="Base retry backoff seconds. Actual backoff doubles on each retry.",
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
        "--fail-on-parse-error-rate",
        type=float,
        default=0.0,
        help="Exit non-zero if parse error rate is greater than this percent.",
    )
    parser.add_argument(
        "--fail-fast-on-parse-error",
        action="store_true",
        help="Stop submitting candidates and fail after the first parse error.",
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
    value = (navii_id or "").strip()
    if len(value) < 4 or not value[:2].isdigit() or not value[2].isdigit():
        return "", "", ""
    return value[:2], value[2:3], value[3:]


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


def open_data_csv_name(kind: str, source_snapshot_date: str) -> str:
    yyyymmdd = source_snapshot_date.replace("-", "")
    if not re.fullmatch(r"\d{8}", yyyymmdd):
        raise ValueError(f"invalid source_snapshot_date: {source_snapshot_date}")
    return OPEN_DATA_FILE_TEMPLATES[kind].format(yyyymmdd=yyyymmdd)


def read_open_data_rows(
    open_data_dir: Path,
    kinds: Iterable[str],
    *,
    source_snapshot_date: str,
) -> list[NaviiCandidate]:
    rows: list[NaviiCandidate] = []
    for kind in kinds:
        csv_name = open_data_csv_name(kind, source_snapshot_date)
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
    sample_fraction: float,
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
        if sample_fraction > 0:
            sample_size = max(1, math.ceil(len(kind_rows) * sample_fraction)) if kind_rows else 0
        else:
            sample_size = sample_per_kind
        if sample_strategy == "prefecture-stratified":
            sampled = stratified_by_prefecture(kind_rows, sample_size)
        else:
            sampled = kind_rows[:sample_size]

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
    "parse_status",
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
    "parse_status",
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
) -> DetailFetchResponse:
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
                return DetailFetchResponse(
                    html=response.read().decode(charset, errors="replace"),
                    status_code=int(response.getcode() or 200),
                    content_type=response.headers.get_content_type() or "",
                    final_url=response.geturl(),
                )
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ssl.SSLError) as exc:
            last_error = exc
            if attempt >= retry_count or not is_retryable_fetch_error(exc):
                raise
            time.sleep(retry_delay_seconds(retry_backoff_seconds, attempt))
    raise RuntimeError(f"unreachable fetch retry state: {last_error}")


def choose_user_agent(*, user_agent: str, user_agent_mode: str, request_seed: int) -> str:
    if user_agent_mode == "rotate-transparent":
        return TRANSPARENT_USER_AGENT_POOL[request_seed % len(TRANSPARENT_USER_AGENT_POOL)]
    return user_agent


def is_retryable_fetch_error(exc: BaseException) -> bool:
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in {408, 409, 425, 429, 500, 502, 503, 504}
    return True


def retry_delay_seconds(base_seconds: float, attempt: int) -> float:
    return base_seconds * (2**attempt)


def require_lxml_html() -> Any:
    try:
        from lxml import html as lxml_html
    except ImportError as exc:
        raise RuntimeError("Navii detail DOM parsing requires lxml. Install lxml in the workflow.") from exc
    return lxml_html


def parse_html_document(page_html: str) -> Any:
    if not page_html.strip():
        raise NaviiParseError("empty detail response")
    lxml_html = require_lxml_html()
    try:
        return lxml_html.document_fromstring(page_html)
    except (TypeError, ValueError) as exc:
        raise NaviiParseError(f"invalid detail HTML: {type(exc).__name__}") from exc


def has_class(element: Any, class_name: str) -> bool:
    classes = set((element.get("class") or "").split())
    return class_name in classes


def inner_html(element: Any) -> str:
    lxml_html = require_lxml_html()
    return "".join(lxml_html.tostring(child, encoding="unicode") for child in element)


def element_text(element: Any) -> str:
    return normalize_text(require_lxml_html().tostring(element, encoding="unicode"))


def structure_fingerprint(tree: Any) -> str:
    """Hash only DOM shape, never page content, for change diagnostics."""
    shape: list[str] = []
    for item in tree.xpath("//div[contains(concat(' ', normalize-space(@class), ' '), ' item ')]"):
        headings = item.xpath("./h2 | ./h3")
        heading = next((candidate for candidate in headings if has_class(candidate, "heading")), None)
        if heading is None and headings:
            heading = headings[0]
        details = item.xpath("./*[contains(concat(' ', normalize-space(@class), ' '), ' details ')]")
        details_node = details[0] if details else None
        heading_tag = heading.tag if heading is not None else "missing"
        heading_classes = " ".join(sorted((heading.get("class") or "").split())) if heading is not None else ""
        details_classes = " ".join(sorted((details_node.get("class") or "").split())) if details_node is not None else ""
        table_count = len(details_node.xpath(".//table")) if details_node is not None else 0
        shape.append(f"item:{heading_tag}:{heading_classes}:details:{details_classes}:tables:{table_count}")
    digest = hashlib.sha256("\n".join(shape).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def discover_sections(page_html: str) -> tuple[Any, list[tuple[str, Any]], str]:
    tree = parse_html_document(page_html)
    sections: list[tuple[str, Any]] = []
    for item in tree.xpath("//div[contains(concat(' ', normalize-space(@class), ' '), ' item ')]"):
        headings = item.xpath("./h2 | ./h3")
        heading = next((candidate for candidate in headings if has_class(candidate, "heading")), None)
        if heading is None and headings:
            heading = headings[0]
        if heading is None:
            continue

        details_id = (heading.get("aria-controls") or "").strip().lstrip("#")
        details: list[Any] = []
        if details_id:
            details = item.xpath(".//*[@id=$details_id]", details_id=details_id)
        if not details:
            details = item.xpath(".//*[contains(concat(' ', normalize-space(@class), ' '), ' details ')]")
        if not details:
            continue

        title = element_text(heading)
        if title:
            sections.append((title, details[0]))

    if not sections:
        item_count = len(tree.xpath("//div[contains(concat(' ', normalize-space(@class), ' '), ' item ')]"))
        raise NaviiParseError(
            f"no target sections found; item_count={item_count}; parser={NAVII_PARSER_VERSION}"
        )
    return tree, sections, structure_fingerprint(tree)


def iter_section_html(page_html: str) -> Iterable[tuple[str, str]]:
    _, sections, _ = discover_sections(page_html)
    for title, details in sections:
        yield title, inner_html(details)


def parse_fragment(fragment: str) -> Any:
    lxml_html = require_lxml_html()
    return lxml_html.fragment_fromstring(fragment, create_parent="div")


def extract_tables_from_element(section_title: str, details: Any) -> list[SectionTable]:
    tables: list[SectionTable] = []
    for table_index, table in enumerate(details.xpath(".//table"), start=1):
        rows: list[list[str]] = []
        for row in table.xpath(".//tr"):
            cells = [element_text(cell) for cell in row.xpath("./th | ./td")]
            cells = [cell for cell in cells if cell]
            if cells:
                rows.append(cells)
        if rows:
            tables.append(SectionTable(section_title=section_title, table_index=table_index, rows=rows))
    return tables


def extract_tables(section_title: str, body_html: str) -> list[SectionTable]:
    return extract_tables_from_element(section_title, parse_fragment(body_html))


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
    return extract_links_from_element(
        section_title=section_title,
        details=parse_fragment(body_html),
        page_url=page_url,
    )


def extract_links_from_element(
    *,
    section_title: str,
    details: Any,
    page_url: str,
) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for table_index, table in enumerate(details.xpath(".//table"), start=1):
        for row_number, row in enumerate(table.xpath(".//tr"), start=1):
            cells = [element_text(cell) for cell in row.xpath("./th | ./td")]
            row_label = next((cell for cell in cells if cell), "")
            raw_row_joined = " | ".join(cell for cell in cells if cell)
            for cell_index, cell in enumerate(row.xpath("./th | ./td"), start=1):
                for anchor in cell.xpath(".//a[@href]"):
                    href_raw = html.unescape(anchor.get("href") or "")
                    if not href_raw:
                        continue
                    link_text = element_text(anchor)
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


def validate_detail_response(response: DetailFetchResponse, candidate: NaviiCandidate) -> None:
    if not 200 <= response.status_code < 300:
        raise NaviiParseError(f"unexpected detail response status={response.status_code}")
    if response.content_type and response.content_type not in {"text/html", "application/xhtml+xml"}:
        raise NaviiParseError(f"unexpected detail content type={response.content_type}")

    expected_query = urllib.parse.parse_qs(urllib.parse.urlparse(candidate.detail_url).query)
    final_query = urllib.parse.parse_qs(urllib.parse.urlparse(response.final_url).query)
    for key, expected in (
        ("prefCd", candidate.pref_cd),
        ("kikanKbn", candidate.kikan_kbn),
        ("kikanCd", candidate.kikan_cd),
    ):
        requested_values = expected_query.get(key, [])
        final_values = final_query.get(key, [])
        if requested_values and requested_values[0] != expected:
            raise NaviiParseError(f"candidate URL identity mismatch for {key}")
        if final_values and final_values[0] != expected:
            raise NaviiParseError(f"redirected URL identity mismatch for {key}")


def parse_error_reason(error: str) -> str:
    if error.startswith("no target sections found"):
        return "no_target_sections"
    if error.startswith("no extractable table rows"):
        return "no_extractable_table_rows"
    if error.startswith("empty detail response"):
        return "empty_response"
    if error.startswith("invalid detail HTML"):
        return "invalid_html"
    if error.startswith("unexpected detail content type"):
        return "unexpected_content_type"
    if "identity mismatch" in error:
        return "identity_mismatch"
    return "parse_error"


def analyze_detail(
    page_html: str,
    *,
    page_url: str = "",
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    summary_rows, table_rows, phone_rows, link_rows, _ = analyze_detail_result(
        page_html,
        page_url=page_url,
    )
    return summary_rows, table_rows, phone_rows, link_rows


def analyze_detail_result(
    page_html: str,
    *,
    page_url: str = "",
) -> tuple[
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    str,
]:
    summary_rows: list[dict[str, str]] = []
    table_rows: list[dict[str, str]] = []
    phone_rows: list[dict[str, str]] = []
    link_rows: list[dict[str, str]] = []

    _, sections, fingerprint = discover_sections(page_html)
    for section_title, details in sections:
        section_text = f"{section_title} {element_text(details)}"
        tables = extract_tables_from_element(section_title, details)
        link_rows.extend(
            extract_links_from_element(
                section_title=section_title,
                details=details,
                page_url=page_url,
            )
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

    return summary_rows, table_rows, phone_rows, link_rows, fingerprint


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

    current_dom_sample = """
    <div class="item">
      <h2 class="heading acHeading" aria-controls="acPnl-current-1">医療機関の人員配置</h2>
      <div class="details" id="acPnl-current-1">
        <table><tbody>
          <tr><th>職種</th><th>総数</th></tr>
          <tr><th>医師</th><td>3</td></tr>
        </tbody></table>
      </div>
    </div>
    <div class="item">
      <h2 class="heading acHeading" aria-controls="acPnl-current-2">電話番号</h2>
      <div id="acPnl-current-2" class="details">
        <table>
          <tr><th>予約用電話番号</th><td><a href="tel:03-1234-5678">03-1234-5678</a></td></tr>
          <tr><th>案内用ホームページアドレス</th><td><a href="/current/">公式サイト</a></td></tr>
        </table>
      </div>
    </div>
    """
    current_summary, current_tables, current_phones, current_links = analyze_detail(
        current_dom_sample,
        page_url="https://example.test/current/",
    )
    assert len(current_tables) == 4, current_tables
    assert current_phones[0]["phone_number_normalized"] == "0312345678", current_phones
    assert any(row["link_href_resolved"] == "https://example.test/current/" for row in current_links)
    assert {row["target_group"] for row in current_summary} >= {ALL_DETAIL_GROUP, "personnel", "phone_contact"}

    try:
        analyze_detail("<html><body><div class='item'><h2>未知の構造</h2></div></body></html>")
    except NaviiParseError:
        pass
    else:
        raise AssertionError("unknown Navii DOM must fail closed")
    print("navii_detail collector self-test passed")


def build_page_coverage_rows(
    candidate: NaviiCandidate,
    fetch_status: str,
    parse_status: str,
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
                "parse_status": parse_status,
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
        response = fetch_detail_html(
            candidate.detail_url,
            user_agent=args.user_agent,
            user_agent_mode=args.user_agent_mode,
            request_seed=index,
            timeout_seconds=args.timeout_seconds,
            insecure_skip_tls_verify=args.insecure_skip_tls_verify,
            retry_count=args.retry_count,
            retry_backoff_seconds=args.retry_backoff_seconds,
        )
        validate_detail_response(response, candidate)
        section_summaries, section_tables, phone_numbers, links, fingerprint = analyze_detail_result(
            response.html,
            page_url=candidate.detail_url,
        )
        if not section_tables:
            raise NaviiParseError("no extractable table rows found")
        summary_rows: list[dict[str, str]] = []
        table_rows: list[dict[str, str]] = []
        phone_rows: list[dict[str, str]] = []
        link_rows: list[dict[str, str]] = []
        for row in section_summaries:
            summary_rows.append(
                {
                    **candidate.__dict__,
                    "fetch_status": "ok",
                    "parse_status": "ok",
                    "error": "",
                    **row,
                }
            )
        for row in section_tables:
            table_rows.append({**candidate.__dict__, **row})
        for row in phone_numbers:
            phone_rows.append({**candidate.__dict__, **row})
        for row in links:
            link_rows.append({**candidate.__dict__, **row})
        page_coverage_rows = build_page_coverage_rows(
            candidate, "ok", "ok", "", section_summaries, section_tables
        )
        return {
            "index": index,
            "candidate": candidate,
            "fetch_status": "ok",
            "parse_status": "ok",
            "parse_error_reason": "",
            "error": "",
            "section_count": sum(
                1 for row in section_summaries if row["target_group"] == ALL_DETAIL_GROUP
            ),
            "table_row_count": len(section_tables),
            "link_row_count": len(link_rows),
            "phone_number_row_count": len(phone_rows),
            "structure_fingerprint": fingerprint,
            "summary_rows": summary_rows,
            "table_rows": table_rows,
            "phone_rows": phone_rows,
            "link_rows": link_rows,
            "page_coverage_rows": page_coverage_rows,
        }
    except NaviiParseError as exc:
        error = str(exc)
        summary_rows = [
            {
                **candidate.__dict__,
                "fetch_status": "ok",
                "parse_status": "error",
                "target_group": "",
                "section_title": "",
                "section_text_sample": "",
                "table_count": "0",
                "has_extractable_table": "false",
                "error": error,
            }
        ]
        page_coverage_rows = build_page_coverage_rows(
            candidate, "ok", "error", error, [], []
        )
        return {
            "index": index,
            "candidate": candidate,
            "fetch_status": "ok",
            "parse_status": "error",
            "parse_error_reason": parse_error_reason(error),
            "error": error,
            "section_count": 0,
            "table_row_count": 0,
            "link_row_count": 0,
            "phone_number_row_count": 0,
            "structure_fingerprint": "",
            "summary_rows": summary_rows,
            "table_rows": [],
            "phone_rows": [],
            "link_rows": [],
            "page_coverage_rows": page_coverage_rows,
        }
    except (urllib.error.URLError, TimeoutError, ssl.SSLError) as exc:
        error = str(exc)
        summary_rows = [
            {
                **candidate.__dict__,
                "fetch_status": "error",
                "parse_status": "not_run",
                "target_group": "",
                "section_title": "",
                "section_text_sample": "",
                "table_count": "0",
                "has_extractable_table": "false",
                "error": error,
            }
        ]
        page_coverage_rows = build_page_coverage_rows(
            candidate, "error", "not_run", error, [], []
        )
        return {
            "index": index,
            "candidate": candidate,
            "fetch_status": "error",
            "parse_status": "not_run",
            "parse_error_reason": "",
            "error": error,
            "section_count": 0,
            "table_row_count": 0,
            "link_row_count": 0,
            "phone_number_row_count": 0,
            "structure_fingerprint": "",
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
    if not args.source_snapshot_date:
        missing.append("--source-snapshot-date")
    if missing:
        raise SystemExit(f"Missing required arguments: {', '.join(missing)}")


def completed_navii_ids_from_page_coverage(path: Path) -> set[str]:
    completed: dict[str, set[str]] = defaultdict(set)
    for row in iter_csv_rows(path):
        if row.get("fetch_status") == "ok" and row.get("parse_status", "ok") == "ok":
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
    invalid = sorted(set(kinds) - set(OPEN_DATA_FILE_TEMPLATES))
    if invalid:
        raise SystemExit(f"Unknown kind(s): {', '.join(invalid)}")
    if not 0 <= args.sample_fraction <= 1:
        raise SystemExit("--sample-fraction must be between 0 and 1")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    all_rows = read_open_data_rows(
        args.open_data_dir,
        kinds,
        source_snapshot_date=args.source_snapshot_date,
    )
    selected_candidates = select_candidates(
        all_rows,
        kinds=kinds,
        sample_per_kind=args.sample_per_kind,
        sample_fraction=args.sample_fraction,
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
            "quality_status": "not_applicable",
            "parser_version": NAVII_PARSER_VERSION,
            "source_id": args.source_id,
            "source_snapshot_date": args.source_snapshot_date,
            "run_label": args.run_label,
            "artifact_mode": args.artifact_mode,
            "selected_candidate_count": len(selected_candidates),
            "shard_candidate_count": len(shard_candidates),
            "candidate_count": len(candidates),
            "available_candidate_count": len(all_rows),
            "available_candidate_counts": dict(Counter(row.source_kind for row in all_rows)),
            "sample_fraction": args.sample_fraction,
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
    kind_metrics: dict[str, Counter[str]] = defaultdict(Counter)
    kind_fingerprints: dict[str, set[str]] = defaultdict(set)
    early_failure_reason = ""

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

        def record_result(result: dict[str, object], completed_count: int) -> None:
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
            counts["section_count"] += int(result["section_count"])
            counts[f"fetch_{result['fetch_status']}"] += 1
            counts[f"parse_{result['parse_status']}"] += 1
            result_candidate = result["candidate"]
            assert isinstance(result_candidate, NaviiCandidate)
            kind_counter = kind_metrics[result_candidate.source_kind]
            kind_counter["candidate_count"] += 1
            kind_counter[f"fetch_{result['fetch_status']}_count"] += 1
            kind_counter[f"parse_{result['parse_status']}_count"] += 1
            kind_counter["section_count"] += int(result["section_count"])
            kind_counter["table_row_count"] += int(result["table_row_count"])
            kind_counter["link_row_count"] += int(result["link_row_count"])
            kind_counter["phone_number_row_count"] += int(result["phone_number_row_count"])
            parse_reason = str(result["parse_error_reason"] or "")
            if parse_reason:
                kind_counter[f"parse_error_reason:{parse_reason}"] += 1
            fingerprint = str(result["structure_fingerprint"] or "")
            if fingerprint:
                kind_fingerprints[result_candidate.source_kind].add(fingerprint)

            if args.progress_every > 0 and completed_count % args.progress_every == 0:
                print(
                    json.dumps(
                        {
                            "progress": completed_count,
                            "submitted_candidate_count": submitted_count,
                            "pending_candidate_count": len(pending_candidates),
                            "shard_index": args.shard_index,
                            "fetch_ok": counts["fetch_ok"],
                            "fetch_error": counts["fetch_error"],
                        },
                        ensure_ascii=False,
                    ),
                    file=sys.stderr,
                )

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=max(args.workers, 1))
        future_to_candidate: dict[concurrent.futures.Future[dict[str, object]], NaviiCandidate] = {}
        candidate_iterator = iter(enumerate(pending_candidates, start=1))
        submitted_count = 0
        completed_count = 0
        aborted = False

        def submit_next() -> bool:
            nonlocal submitted_count
            try:
                index, candidate = next(candidate_iterator)
            except StopIteration:
                return False
            if submitted_count > 0:
                delay = args.pause_seconds
                if args.jitter_seconds > 0:
                    delay += random.uniform(0, args.jitter_seconds)
                if delay > 0:
                    time.sleep(delay)
            future = executor.submit(process_candidate, index=index, candidate=candidate, args=args)
            future_to_candidate[future] = candidate
            submitted_count += 1
            return True

        try:
            for _ in range(min(max(args.workers, 1), len(pending_candidates))):
                submit_next()

            while future_to_candidate:
                done, _ = concurrent.futures.wait(
                    future_to_candidate,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    result = future.result()
                    completed_count += 1
                    record_result(result, completed_count)
                    result_candidate = result["candidate"]
                    assert isinstance(result_candidate, NaviiCandidate)
                    if args.fail_fast_on_parse_error and result["parse_status"] == "error":
                        reason = str(result["parse_error_reason"] or "unknown")
                        early_failure_reason = (
                            "fail-fast parse error at "
                            f"{result_candidate.source_kind}/{result_candidate.navii_id} ({reason})"
                        )
                        aborted = True
                        break
                    del future_to_candidate[future]
                    submit_next()
                if aborted:
                    break
        except BaseException:
            aborted = True
            raise
        finally:
            if aborted:
                for future in future_to_candidate:
                    future.cancel()
            executor.shutdown(wait=True, cancel_futures=aborted)
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
    parse_attempt_count = counts["parse_ok"] + counts["parse_error"]
    parse_error_rate = (counts["parse_error"] / parse_attempt_count) if parse_attempt_count else 0.0
    parse_error_rate_percent = parse_error_rate * 100
    quality_failures: list[str] = []
    if early_failure_reason:
        quality_failures.append(early_failure_reason)
    if fetch_error_rate_percent > args.fail_on_fetch_error_rate:
        quality_failures.append(
            f"fetch_error_rate {fetch_error_rate_percent:.2f}% exceeded {args.fail_on_fetch_error_rate:.2f}%"
        )
    if parse_error_rate_percent > args.fail_on_parse_error_rate:
        quality_failures.append(
            f"parse_error_rate {parse_error_rate_percent:.2f}% exceeded {args.fail_on_parse_error_rate:.2f}%"
        )
    if counts["parse_ok"] and counts["table_rows"] == 0:
        quality_failures.append("no extractable table rows after successful parsing")
    serialized_kind_metrics = {
        kind: {
            **{
                key: int(counter.get(key, 0))
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
                )
            },
            "structure_fingerprints": sorted(kind_fingerprints.get(kind, set())),
            "parse_error_reasons": {
                key.split(":", 1)[1]: int(value)
                for key, value in counter.items()
                if key.startswith("parse_error_reason:")
            },
        }
        for kind, counter in sorted(kind_metrics.items())
    }
    completed_at = utc_now_iso()
    metrics = {
        "schema_version": "1.0",
        "status": "executed",
        "quality_status": "fail" if quality_failures else "pass",
        "quality_failures": quality_failures,
        "parser_version": NAVII_PARSER_VERSION,
        "source_id": args.source_id,
        "source_snapshot_date": args.source_snapshot_date,
        "run_label": args.run_label,
        "artifact_mode": args.artifact_mode,
        "selected_candidate_count": len(selected_candidates),
        "shard_candidate_count": len(shard_candidates),
        "candidate_count": len(candidates),
        "available_candidate_count": len(all_rows),
        "available_candidate_counts": dict(Counter(row.source_kind for row in all_rows)),
        "sample_fraction": args.sample_fraction,
        "selected_candidate_fraction": (
            len(selected_candidates) / len(all_rows) if all_rows else 0.0
        ),
        "pending_candidate_count": len(pending_candidates),
        "resumed_candidate_count": len(completed_ids),
        "output_candidate_count": len(completed_ids) + total_fetch_count,
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "workers": args.workers,
        "pause_seconds": args.pause_seconds,
        "jitter_seconds": args.jitter_seconds,
        "retry_count": args.retry_count,
        "fail_fast_on_parse_error": args.fail_fast_on_parse_error,
        "early_failure_reason": early_failure_reason,
        "fetch_ok_count": counts["fetch_ok"],
        "fetch_error_count": counts["fetch_error"],
        "fetch_error_rate": round(fetch_error_rate, 6),
        "fetch_error_rate_percent": round(fetch_error_rate_percent, 4),
        "parse_ok_count": counts["parse_ok"],
        "parse_error_count": counts["parse_error"],
        "parse_error_rate": round(parse_error_rate, 6),
        "parse_error_rate_percent": round(parse_error_rate_percent, 4),
        "started_at": started_at,
        "completed_at": completed_at,
        "summary_rows": counts["summary_rows"],
        "section_count": counts["section_count"],
        "table_rows": counts["table_rows"],
        "link_rows": counts["link_rows"],
        "phone_number_rows": counts["phone_number_rows"],
        "page_coverage_rows": counts["page_coverage_rows"],
        "kind_metrics": serialized_kind_metrics,
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
    if quality_failures:
        print("Navii detail quality gate failed: " + "; ".join(quality_failures), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
