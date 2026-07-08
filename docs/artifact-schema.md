# Artifact Schema

public source snapshotterからconsumerへ渡すartifact contractです。

public repoのartifactは第三者に見られる前提で扱います。summary / metricsは平文でよい一方、施設単位のfull artifactは暗号化します。

## Navii Detail Package Layout

```text
run-{source_id}-{source_snapshot_date}-{run_label}/
  manifest/
    source-snapshot-manifest.json
    collector-run-manifest.json
  metrics/
    fetch-metrics.json
    shard-summary.json
    coverage-summary.csv
  checksums/
    SHA256SUMS
  encrypted/
    raw-artifacts-shard-000.tar.zst.age
```

`summary_only` canaryでは `encrypted/raw-artifacts-shard-*.tar.zst.age` は作らず、summary / metrics / manifest / checksumだけを確認します。

## `collector-run-manifest.json`

```json
{
  "schema_version": "1.0",
  "collector_repo": "owner/repository",
  "source_id": "navii_detail",
  "source_snapshot_date": "2026-06-01",
  "run_label": "collector-navii-detail-20260601-canary",
  "github_run_id": "123456789",
  "github_run_url": "https://github.com/owner/repository/actions/runs/123456789",
  "completed_at": "2026-06-01T00:00:00Z",
  "artifact_mode": "summary_only",
  "candidate_selection": {
    "mode": "all",
    "sample_per_kind": 25,
    "sample_strategy": "prefecture-stratified"
  },
  "shards": {
    "shard_count": 1,
    "max_parallel": 1,
    "workers_per_shard": 2
  }
}
```

## `fetch-metrics.json`

```json
{
  "schema_version": "1.0",
  "source_id": "navii_detail",
  "source_snapshot_date": "2026-06-01",
  "candidate_count": 400,
  "fetch_ok_count": 400,
  "fetch_error_count": 0,
  "fetch_error_rate": 0,
  "retry_count": 2,
  "timeout_seconds": 30,
  "pause_seconds": 0.3,
  "jitter_seconds": 0.1,
  "user_agent_mode": "rotate-transparent"
}
```

## `shard-summary.json`

```json
{
  "schema_version": "1.0",
  "source_id": "navii_detail",
  "source_snapshot_date": "2026-06-01",
  "shard_count": 1,
  "shards": [
    {
      "shard_index": 0,
      "candidate_count": 400,
      "fetch_ok_count": 400,
      "fetch_error_count": 0,
      "artifact_dir": "raw-artifacts/shard-000"
    }
  ]
}
```

## Raw Artifact Contents

暗号化前のfull artifactには、少なくとも次を含めます。

```text
raw-artifacts/
  shard-000/
    candidates.csv.gz
    page-coverage.csv.gz
    table-rows.csv.gz
    links.csv.gz
    phone-numbers.csv.gz
    summary.csv.gz
    coverage-summary.csv.gz
    run-metrics.json
```

`table-rows.csv.gz` はdetail page内の抽出可能なtable rowを全section分出力します。これらのrowは `target_group=all_detail` を使います。

`links.csv.gz` はdetail page内tableに含まれるlinkのvisible text、raw href、resolved hrefをsource-neutralに記録します。

shard job間の中間artifactもpublic repo上では見える前提になるため、`encrypted_full` ではshardごとに暗号化したartifactだけをuploadします。未暗号化のraw shard directoryはpackage artifactへ含めません。

## MHLW Monthly Package Layout

厚労省月次source取得では、raw source fileをparseせず、download pathとchecksumだけを渡します。

```text
manifest/
  source-snapshot-manifest.json
  collector-run-manifest.json
metrics/
  fetch-metrics.json
  mhlw-source-file-inventory.csv
  source-coverage-summary.csv
checksums/
  SHA256SUMS
encrypted/
  raw-mhlw-source-files.tar.zst.age
```

暗号化前のfull artifactは次の形です。

```text
raw-files/
  近畿/
    届出受理/
      kinki-medical-todokede.zip
```

`summary_only` では `raw-files/` はuploadしません。

## Public Log Boundary

logに出してよい:

- source id
- source snapshot date
- run label
- shard index
- candidate count
- fetch ok / error count
- checksum
- artifact file name

logに出さない:

- raw HTML本文
- raw full CSV / JSONL本文
- 施設単位の全件table rows
- 復号済みartifact本文
- operator-only filesystem paths
- downstream candidate CSV
- downstream processing result
- secret / token
