"""Tests for the Navii detail DOM parser and response identity checks."""

from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from collectors.navii_detail.collect import (
    ALL_DETAIL_GROUP,
    DetailFetchResponse,
    NaviiCandidate,
    NaviiDetailIdentifierNotFound,
    NaviiSearchMatch,
    NaviiSearchError,
    NaviiParseError,
    analyze_detail,
    analyze_detail_result,
    load_detail_url_overrides,
    parse_navii_search_matches,
    parse_navii_id,
    process_candidate,
    retry_delay_seconds,
    resolve_candidate_url,
    select_candidates,
    validate_detail_response,
)


CURRENT_DOM = """
<html><body>
  <div class="item">
    <h2 aria-controls="acPnl-current-1" class="acHeading heading">
      医療機関の人員配置
    </h2>
    <div id="acPnl-current-1" class="details">
      <div class="wrapper"><table><tbody>
        <tr><th>職種</th><th>総数</th></tr>
        <tr><th>医師</th><td>3</td></tr>
      </tbody></table></div>
    </div>
  </div>
  <div class="item">
    <h2 class="heading acHeading" aria-controls="acPnl-current-2">電話番号</h2>
    <div class="details extra" id="acPnl-current-2">
      <table>
        <tr><th>予約用電話番号</th><td><a href="tel:03-1234-5678">03-1234-5678</a></td></tr>
        <tr><th>案内用ホームページアドレス</th><td><a href="/current/">公式サイト</a></td></tr>
      </table>
    </div>
  </div>
</body></html>
"""


LEGACY_DOM = """
<div class="item">
  <h3 class="heading acHeading"><a><div>医療機関の人員配置</div></a></h3>
  <div class="details idx-20"><table>
    <tr><th>職種</th><th>総数</th></tr>
    <tr><th>医師</th><td>2</td></tr>
  </table></div><!-- /.details -->
</div>
"""


class NaviiDetailParserTest(unittest.TestCase):
    def test_sample_fraction_selects_deterministic_rows_per_kind(self) -> None:
        rows = [
            NaviiCandidate(
                source_kind=kind,
                product_slug=kind,
                navii_id=f"{kind}-{index}",
                pref_cd=f"{index % 47 + 1:02d}",
                kikan_kbn="1",
                kikan_cd=f"{index:09d}",
                name=f"{kind}-{index}",
                address="",
                detail_url="https://example.test/detail",
            )
            for kind in ("hospital", "pharmacy")
            for index in range(100)
        ]

        selected = select_candidates(
            rows,
            kinds=["hospital", "pharmacy"],
            sample_per_kind=25,
            sample_fraction=0.1,
            sample_strategy="first",
            navii_ids=set(),
            all_candidates=False,
        )

        self.assertEqual(len(selected), 20)
        self.assertEqual(
            {kind: sum(row.source_kind == kind for row in selected) for kind in ("hospital", "pharmacy")},
            {"hospital": 10, "pharmacy": 10},
        )

    def test_navii_id_parser_preserves_alphanumeric_facility_suffix(self) -> None:
        self.assertEqual(parse_navii_id("37537X5122198"), ("37", "5", "37X5122198"))
        self.assertEqual(parse_navii_id("375L370100040"), ("37", "5", "L370100040"))

    def test_retry_backoff_is_exponential(self) -> None:
        self.assertEqual(retry_delay_seconds(2, 0), 2)
        self.assertEqual(retry_delay_seconds(2, 1), 4)
        self.assertEqual(retry_delay_seconds(2, 2), 8)

    def test_current_h2_dom_and_id_relation_are_extracted(self) -> None:
        summary, tables, phones, links, fingerprint = analyze_detail_result(
            CURRENT_DOM,
            page_url="https://example.test/current/",
        )

        self.assertEqual(len([row for row in summary if row["target_group"] == ALL_DETAIL_GROUP]), 2)
        self.assertEqual(len(tables), 4)
        self.assertEqual(phones[0]["phone_number_normalized"], "0312345678")
        self.assertTrue(any(row["link_href_resolved"] == "https://example.test/current/" for row in links))
        self.assertTrue(fingerprint.startswith("sha256:"))

    def test_legacy_h3_fixture_remains_supported(self) -> None:
        summary, tables, phones, links = analyze_detail(LEGACY_DOM)

        self.assertTrue(summary)
        self.assertEqual(len(tables), 2)
        self.assertFalse(phones)
        self.assertFalse(links)

    def test_unknown_dom_fails_closed(self) -> None:
        with self.assertRaisesRegex(NaviiParseError, "no target sections found"):
            analyze_detail("<html><body><div class='item'><h2>unknown</h2></div></body></html>")

    def test_response_identity_rejects_redirected_facility(self) -> None:
        candidate = NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="0120116711805",
            pref_cd="01",
            kikan_kbn="2",
            kikan_cd="0116711805",
            name="fixture clinic",
            address="北海道",
            detail_url=(
                "https://www.iryou.teikyouseido.mhlw.go.jp/znk-web/juminkanja/"
                "S2430/initialize?prefCd=01&kikanKbn=2&kikanCd=0116711805"
            ),
        )
        response = DetailFetchResponse(
            html=CURRENT_DOM,
            status_code=200,
            content_type="text/html",
            final_url=(
                "https://www.iryou.teikyouseido.mhlw.go.jp/znk-web/juminkanja/"
                "S2430/initialize?prefCd=01&kikanKbn=5&kikanCd=0116711805"
            ),
        )

        with self.assertRaisesRegex(NaviiParseError, "identity mismatch"):
            validate_detail_response(response, candidate)

    def test_response_identity_rejects_non_html(self) -> None:
        candidate = NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="0120116711805",
            pref_cd="01",
            kikan_kbn="2",
            kikan_cd="0116711805",
            name="fixture clinic",
            address="北海道",
            detail_url="https://example.test?prefCd=01&kikanKbn=2&kikanCd=0116711805",
        )
        response = DetailFetchResponse(
            html="{}",
            status_code=200,
            content_type="application/json",
            final_url=candidate.detail_url,
        )

        with self.assertRaisesRegex(NaviiParseError, "content type"):
            validate_detail_response(response, candidate)

    def test_e0109_is_classified_as_missing_detail_identifier(self) -> None:
        candidate = NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="1021012511471",
            pref_cd="10",
            kikan_kbn="2",
            kikan_cd="1012511471",
            name="いじま内科・消化器内科",
            address="群馬県太田市飯塚町",
            detail_url="https://example.test/detail",
        )
        response = DetailFetchResponse(
            html="<html><body>指定されたデータは存在しません。[1012511471](E-0109)</body></html>",
            status_code=200,
            content_type="text/html",
            final_url=candidate.detail_url,
        )

        with self.assertRaises(NaviiDetailIdentifierNotFound):
            validate_detail_response(response, candidate)

    def test_missing_identifier_phrase_without_error_code_is_classified(self) -> None:
        candidate = NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="1021012511471",
            pref_cd="10",
            kikan_kbn="2",
            kikan_cd="1012511471",
            name="いじま内科・消化器内科",
            address="群馬県太田市飯塚町",
            detail_url="https://example.test/detail",
        )
        response = DetailFetchResponse(
            html="<html><body>指定されたデータは存在しません。</body></html>",
            status_code=200,
            content_type="text/html",
            final_url=candidate.detail_url,
        )

        with self.assertRaises(NaviiDetailIdentifierNotFound):
            validate_detail_response(response, candidate)

    def test_override_map_resolves_known_exception_before_derived_url(self) -> None:
        overrides = load_detail_url_overrides()
        candidate = NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="1021012511471",
            pref_cd="10",
            kikan_kbn="2",
            kikan_cd="1012511471",
            name="いじま内科・消化器内科",
            address="群馬県太田市飯塚町",
            detail_url="",
        )

        resolution = resolve_candidate_url(candidate, overrides)

        self.assertEqual(resolution.status, "override_hit")
        self.assertIn("2100002055", resolution.request_candidate.detail_url)
        self.assertIn("1012511471", resolution.derived_url)

    def test_search_result_parser_extracts_identity_and_address(self) -> None:
        html = """
        <div class="resultItems"><div class="item">
          <h2 class="name"><a href="/znk-web/juminkanja/S2430/initialize?prefCd=10&amp;kikanCd=2100002055&amp;kikanKbn=2">
            いじま内科・消化器内科
          </a></h2>
          <dl><div><dt><img alt="住所"></dt><dd><p>〒373-0817 群馬県太田市飯塚町</p></dd></div></dl>
        </div></div>
        """

        matches = parse_navii_search_matches(html)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].name, "いじま内科・消化器内科")
        self.assertEqual(matches[0].address, "群馬県太田市飯塚町")
        self.assertEqual(matches[0].kikan_cd, "2100002055")

    def test_unresolved_candidate_keeps_source_fields_and_blanks_navii_fields(self) -> None:
        candidate = NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="1021012511471",
            pref_cd="10",
            kikan_kbn="2",
            kikan_cd="1012511471",
            name="いじま内科・消化器内科",
            address="群馬県太田市飯塚町",
            detail_url="https://example.test/detail",
        )
        args = Namespace(
            detail_url_overrides={},
            user_agent="test",
            user_agent_mode="fixed",
            timeout_seconds=1,
            insecure_skip_tls_verify=False,
            retry_count=0,
            retry_backoff_seconds=0,
        )
        missing_response = DetailFetchResponse(
            html="<html><body>指定されたデータは存在しません。(E-0109)</body></html>",
            status_code=200,
            content_type="text/html",
            final_url=candidate.detail_url,
        )

        with patch(
            "collectors.navii_detail.collect.fetch_detail_html",
            return_value=missing_response,
        ), patch(
            "collectors.navii_detail.collect.search_navii_exact",
            side_effect=NaviiSearchError("Navii exact search did not return one exact facility result: count=0"),
        ):
            result = process_candidate(index=1, candidate=candidate, args=args)

        self.assertEqual(result["resolution_status"], "unresolved")
        self.assertEqual(result["candidate"].name, candidate.name)
        self.assertEqual(result["candidate"].detail_url, "")
        self.assertEqual(result["table_rows"], [])
        self.assertEqual(result["summary_rows"][0]["navii_detail_status"], "unresolved")
        self.assertEqual(result["summary_rows"][0]["navii_detail_reason"], "exact_search_no_unique_match")

    def test_search_resolved_candidate_is_proposed_for_map_promotion(self) -> None:
        candidate = NaviiCandidate(
            source_kind="clinic",
            product_slug="medical",
            navii_id="1021012511471",
            pref_cd="10",
            kikan_kbn="2",
            kikan_cd="1012511471",
            name="いじま内科・消化器内科",
            address="群馬県太田市飯塚町",
            detail_url="https://example.test/detail",
        )
        match_url = (
            "https://www.iryou.teikyouseido.mhlw.go.jp/znk-web/juminkanja/S2430/initialize"
            "?prefCd=10&kikanKbn=2&kikanCd=2100002055"
        )
        args = Namespace(
            detail_url_overrides={},
            user_agent="test",
            user_agent_mode="fixed",
            timeout_seconds=1,
            insecure_skip_tls_verify=False,
            retry_count=0,
            retry_backoff_seconds=0,
        )
        missing_response = DetailFetchResponse(
            html="<html><body>指定されたデータは存在しません。(E-0109)</body></html>",
            status_code=200,
            content_type="text/html",
            final_url=candidate.detail_url,
        )
        valid_response = DetailFetchResponse(
            html=CURRENT_DOM,
            status_code=200,
            content_type="text/html",
            final_url=match_url,
        )

        with patch(
            "collectors.navii_detail.collect.fetch_detail_html",
            side_effect=[missing_response, valid_response],
        ), patch(
            "collectors.navii_detail.collect.search_navii_exact",
            return_value=NaviiSearchMatch(
                name=candidate.name,
                address=candidate.address,
                pref_cd="10",
                kikan_kbn="2",
                kikan_cd="2100002055",
                detail_url=match_url,
            ),
        ):
            result = process_candidate(index=1, candidate=candidate, args=args)

        self.assertEqual(result["resolution_status"], "search_resolved")
        self.assertEqual(result["candidate"].detail_url, match_url)
        self.assertEqual(result["proposed_override"]["kikan_cd"], "2100002055")
        self.assertIn("kikanCd=1012511471", result["proposed_override"]["previous_detail_url"])
        self.assertEqual(result["parse_status"], "ok")

    def test_override_map_rejects_duplicate_or_mismatched_query_identity(self) -> None:
        valid = {
            "source_kind": "clinic",
            "source_id": "1021012511471",
            "pref_cd": "10",
            "kikan_kbn": "2",
            "kikan_cd": "2100002055",
            "facility_name": "いじま内科・消化器内科",
            "address": "群馬県太田市飯塚町",
            "reason": "fixture",
            "verified_at": "2026-08-31",
            "detail_url": "https://www.iryou.teikyouseido.mhlw.go.jp/znk-web/juminkanja/S2430/initialize?prefCd=10&kikanKbn=2&kikanCd=2100002055",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "overrides.json"
            path.write_text(
                json.dumps({"schema_version": "1.0", "overrides": [valid, valid]}, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate detail URL override key"):
                load_detail_url_overrides(path)

            mismatched = dict(valid)
            mismatched["kikan_cd"] = "wrong"
            path.write_text(
                json.dumps({"schema_version": "1.0", "overrides": [mismatched]}, ensure_ascii=False),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "query identity mismatch"):
                load_detail_url_overrides(path)


if __name__ == "__main__":
    unittest.main()
