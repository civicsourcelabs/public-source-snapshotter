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

## Manifest source

Full manifestはprivate registryから公開取得に必要なfieldだけをexportし、このrepoにPRで取り込みます。

このdirectoryの `source-manifest.json` はcollector canary用の最小manifestです。
