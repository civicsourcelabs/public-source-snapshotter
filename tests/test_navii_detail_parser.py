"""Tests for the Navii detail DOM parser and response identity checks."""

from __future__ import annotations

import unittest

from collectors.navii_detail.collect import (
    ALL_DETAIL_GROUP,
    DetailFetchResponse,
    NaviiCandidate,
    NaviiParseError,
    analyze_detail,
    analyze_detail_result,
    parse_navii_id,
    retry_delay_seconds,
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


if __name__ == "__main__":
    unittest.main()
