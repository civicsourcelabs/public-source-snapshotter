# MHLW Monthly Collector

`sources/mhlw_monthly/source-manifest.json` を読み、厚労省地方厚生局の月次source fileを取得します。

このcollectorが行うこと:

- manifest validation
- `direct_download` / `xpath` / `month_context` source resolution
- raw source file download
- SHA256 checksum
- source unit manifest generation
- shallow XLSX content probe for source category / source kind checks
- source-level metrics
- handoff package生成

このcollectorが行わないこと:

- record-level transformation
- downstream item normalization
- downstream source reconciliation
- downstream load script生成
- production DB write
- downstream processing policy

主な出力:

- `manifest/mhlw-source-units.json`
- `manifest/mhlw-source-coverage.json`
- `metrics/mhlw-source-file-inventory.csv`
- `metrics/source-coverage-summary.csv`

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

schedule / manual runともに、公式ページで確認できる対象sourceのうち、Asia/Tokyoの実行時点以前で最も新しい共通snapshotを選び、その `YYYY-MM-01` を `source_snapshot_date` として記録します。実行月に公開されていない場合も、実行月を仮定しません。full条件では選択した日付から `collector-mhlw-monthly-YYYYMM-full` のrun labelを作り、`encrypted_full` artifactを作ります。
