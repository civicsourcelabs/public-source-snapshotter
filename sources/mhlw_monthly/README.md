# MHLW Monthly Source Manifest

厚労省月次source取得用のmanifestです。

このmanifestは公開sourceの取得だけを表します。private datastore、private processing result、load script、private-only recordsは含めません。

## Source row contract

`source_urls` の各rowは、公開source取得に必要な情報だけを持ちます。

```json
{
  "source_key": "kinki-medical-todokede",
  "pipeline_slug": "medical",
  "region": "近畿",
  "source_label": "届出受理_医科",
  "source_type": "todokede",
  "fetch_type": "xpath",
  "page_url": "https://...",
  "xpath": "//a[contains(@href, \"sisetukijun_ika\") and contains(@href, \".zip\")]",
  "download_subdir": "近畿/届出受理",
  "expected_filename": "kinki-medical-todokede.zip"
}
```

`expected_filename` が空の場合、collectorはresolved URLのbasenameを使います。

`month_context` は、同じpageに複数の年月ブロックがあるsource向けです。

- `source_snapshot_date` は `YYYY-MM` として解釈します。
- 直前の令和年月、ファイル形式見出し、link text、hrefをsource linkごとに記録します。
- Excel section内で、対象年月、source title、product labelが一致するlinkを1件だけ選びます。
- 選んだZIP basenameが対象年月とsource type別のファイル名パターンに合わない場合は失敗します。

## Manifest source

Full manifestはprivate registryから公開取得に必要なfieldだけをexportし、このrepoにPRで取り込みます。

このdirectoryの `source-manifest.json` はcollector canary用の最小manifestです。
