# MHLW Monthly Collector

`sources/mhlw_monthly/source-manifest.json` を読み、厚労省地方厚生局の月次source fileを取得します。

このcollectorが行うこと:

- manifest validation
- `direct_download` / `xpath` / `month_context` source resolution
- raw source file download
- SHA256 checksum
- source-level metrics
- handoff package生成

このcollectorが行わないこと:

- Excel parsing
- private item normalization
- private source reconciliation
- private load script生成
- production DB write
- private processing policy

## Local canary

```bash
python3 collectors/mhlw_monthly/collect.py --self-test

python3 collectors/mhlw_monthly/collect.py \
  --manifest sources/mhlw_monthly/source-manifest.json \
  --out-dir /tmp/mhlw-monthly-source-canary \
  --max-sources 1
```

`--execute` を付けない場合はresolved URLとmetricsだけを出し、raw file downloadは行いません。

## GitHub Actions schedule

`.github/workflows/mhlw-monthly-source-snapshot.yml` のschedule runは、manual canary後の通常運用として使います。

- 毎月5日 07:00 JST: private monthly processing前のfull artifact生成
- 毎月8日 07:00 JST: 5日run失敗時のpublic artifact再生成

schedule runではAsia/Tokyoの実行月から `YYYY-MM-01` の `source_snapshot_date` を作り、`collector-mhlw-monthly-YYYYMM-full` のrun labelで `encrypted_full` artifactを作ります。
