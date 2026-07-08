# Navii Detail Collector Contract

public repo側で実装するcollectorのCLI contractです。

`collect.py` は公式open data ZIPからdetail URL候補を作り、owner承認時だけdetail HTMLを取得するpublic-source collectorです。external datastore、external-service secret、deploy provider、production secretには接続しません。

## CLI

```bash
python3 collectors/navii_detail/collect.py \
  --source-dir "$SOURCE_DIR" \
  --out-dir "$OUT_DIR" \
  --source-id navii_detail \
  --source-snapshot-date "$SOURCE_SNAPSHOT_DATE" \
  --run-label "$RUN_LABEL" \
  --artifact-mode summary_only \
  --kinds hospital,clinic,dental,pharmacy \
  --shard-count 1 \
  --shard-index 0 \
  --max-pages-per-shard 400 \
  --workers 2 \
  --pause-seconds 0.3 \
  --jitter-seconds 0.1 \
  --timeout-seconds 30 \
  --retry-count 2 \
  --retry-backoff-seconds 2 \
  --fail-on-fetch-error-rate 5 \
  --execute
```

## Inputs

- official open data ZIPs under `--source-dir`
- source manifest under `sources/navii/source-manifest.json`
- workflow-derived `source_snapshot_date` and `run_label`

`--insecure-skip-tls-verify` is for owner-approved TLS certificate troubleshooting only. Do not use it as the default path.

The collector must not require:

- external datastore
- payment provider
- deploy provider
- downstream candidate CSV
- service role
- production secret

`source_snapshot_date` and `run_label` are derived by
`collectors/navii_detail/source_readiness.py` in the workflow. They are not
normal workflow inputs.

## Outputs

`--out-dir` should contain:

```text
candidates.csv.gz
summary.csv.gz
page-coverage.csv.gz
table-rows.csv.gz
links.csv.gz
phone-numbers.csv.gz
coverage-summary.csv.gz
run-metrics.json
```

`run-metrics.json` must include:

- `schema_version`
- `source_id`
- `source_snapshot_date`
- `run_label`
- `shard_count`
- `shard_index`
- `candidate_count`
- `fetch_ok_count`
- `fetch_error_count`
- `fetch_error_rate`
- `started_at`
- `completed_at`

`phone-numbers.csv.gz` contains neutral phone contact rows extracted from target
detail-page tables. Fax rows are excluded when the row label identifies them as
fax/facsimile rows.

`table-rows.csv.gz` contains all extractable detail-page table rows. These rows
use `target_group=all_detail`; target-group coverage remains available in
`summary.csv.gz`, `page-coverage.csv.gz`, and `coverage-summary.csv.gz`.

`links.csv.gz` contains source links found inside detail-page tables, including
the source section, row label, visible text, raw href, and resolved href.

## Public Safety

The collector may write facility-level rows to local shard artifact files, but the workflow must encrypt full artifacts before long-lived public artifact retention.

`summary_only` modeでは、workflowへuploadされるshard packageにraw facility-level full outputを含めません。`encrypted_full` modeでも、raw outputはshard job内で暗号化してからuploadします。

The collector must not write:

- external datastore ids
- downstream processing confidence
- owner review notes
- datastore load script
- secret / token values
