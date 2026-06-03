# Collector Runbook

このpublic repoでsource snapshotを取得するためのrunbookです。

このrunbookはpublic collector側だけを扱います。private candidate出力、private processing inventory、private policy反映、private loadは扱いません。

## Supported collectors

| collector | workflow | role |
| --- | --- | --- |
| Navii detail | `Source Snapshot: Navii Detail` | ナビィopen dataからdetail pageを取得し、人員・在宅/連携・予約/診療時間系tableを抽出する |
| MHLW monthly | `Source Snapshot: MHLW Monthly` | 厚労省地方厚生局の月次source ZIP/XLSXを取得し、checksum付きraw source packageを作る |

## Schedule policy

MHLW monthly collectorとNavii detail collectorは、private processing scheduleより前にencrypted full artifactを生成します。

| workflow | schedule | effective config |
| --- | --- | --- |
| `Source Snapshot: MHLW Monthly` | 毎月5日 07:00 JST / 毎月8日 07:00 JST | `execute=true`、`artifact_mode=encrypted_full`、`max_sources=0`、`source_manifest=sources/mhlw_monthly/source-manifest.json` |
| `Source Snapshot: Navii Detail` | 毎月4日 00:30 JST | `execute=true`、`artifact_mode=encrypted_full`、`shard_count=32`、`max_parallel=32`、`max_pages_per_shard=0`、`artifact_retention_days=14` |

schedule runでは、Asia/Tokyoの実行月から `YYYY-MM-01` の `source_snapshot_date` と `collector-mhlw-monthly-YYYYMM-full` のrun labelを自動生成します。private processing側は同日後続scheduleで最新成功full artifactを解決して使うため、public repo側の5日runが失敗した場合は8日runで再生成します。

Navii detail schedule runでは、`sources/navii/source-manifest.json` の `source_snapshot_date` から `collector-navii-detail-YYYYMMDD-full` のrun labelを自動生成します。private prepare-load consumerは毎月5日 04:20 JSTに最新成功full artifactを解決して使うため、public repo側のNavii full runは前日深夜に完了する想定です。

## 初回セットアップ

完了済み:

- ownerがpublic repoを作成する
- scaffoldをrepo rootへ転記し、初回empty repo bootstrapを `main` へpushする
- collector実装 `collectors/navii_detail/collect.py` を追加する

未完了:

1. `sources/navii/source-manifest.json` のsource snapshot dateとURLを確認する
2. `summary_only` canaryを実行する
3. `keys/owner-age-recipient.example.txt` を `keys/owner-age-recipient.txt` に置き換える
4. `encrypted_full` canaryを実行する

private keyはpublic repoへ置きません。GitHub secretにも登録しません。

collector実装追加とcanary成功は別です。canary結果を確認するまでは、取得導線が本番運用可能とは扱いません。

## Navii Canary 1: summary only

目的は、public repo側のworkflow、source manifest、shard matrix、summary artifactが動くかだけを確認することです。

推奨input:

| input | value |
| --- | --- |
| `execute` | `false` |
| `confirm` | 空 |
| `source_snapshot_date` | `2025-12-01` |
| `run_label` | `collector-navii-detail-20251201-canary` |
| `artifact_mode` | `summary_only` |
| `shard_count` | `1` |
| `max_parallel` | `1` |
| `max_pages_per_shard` | `400` |

確認:

- workflowがmanual dispatchでだけ起動する
- source ZIPをdownloadできる
- `collector-run-manifest.json` が作られる
- `SHA256SUMS` が作られる
- workflow logにraw HTMLや施設単位full dataが出ていない
- `encrypted/README.txt` 以外のraw full artifactが平文でuploadされていない

## Navii Canary 2: encrypted full

目的は、full artifactを暗号化し、owner localで復号できることを確認することです。

推奨input:

| input | value |
| --- | --- |
| `execute` | `true` |
| `confirm` | `owner-approved-public-source-snapshot` |
| `artifact_mode` | `encrypted_full` |
| `shard_count` | `1` |
| `max_parallel` | `1` |
| `workers` | `2` |
| `pause_seconds` | `0.3` |
| `jitter_seconds` | `0.1` |
| `insecure_skip_tls_verify` | `false` |
| `max_pages_per_shard` | `400` |

確認:

- `encrypted/raw-artifacts-shard-*.tar.zst.age` がある
- `SHA256SUMS` とartifactが一致する
- owner localで復号できる
- 復号済みartifactのrow countがmanifest / metricsと一致する
- private normalizer intakeに渡せる
- 暗号化前の `*.tar.zst` やraw CSV / JSONL / HTMLがGitHub artifactへ残っていない

TLS certificate verificationだけが失敗する場合に限り、owner判断で `insecure_skip_tls_verify=true` を使います。通常runでは `false` のままにします。

## Navii Detail Full Run

manual full runは、schedule失敗時の再生成、manifest更新直後の確認、またはprivate prepare-load artifactを固定したい場合に使います。canaryとowner local decrypt確認が済んでいない状態では実行しません。

推奨input:

| input | value |
| --- | --- |
| `execute` | `true` |
| `confirm` | `owner-approved-public-source-snapshot` |
| `source_snapshot_date` | `sources/navii/source-manifest.json` の `source_snapshot_date` |
| `run_label` | `collector-navii-detail-YYYYMMDD-full` |
| `artifact_mode` | `encrypted_full` |
| `shard_count` | `32` |
| `max_parallel` | `32` |
| `workers` | `2` |
| `pause_seconds` | `0.3` |
| `jitter_seconds` | `0.1` |
| `max_pages_per_shard` | `0` |
| `artifact_retention_days` | `14` |

schedule runの期待値:

- `Validate inputs` stepで `execute=true`、`artifact_mode=encrypted_full`、`shard_count=32`、`max_pages_per_shard=0` になっている
- `Package handoff artifact` stepで `encrypted/raw-artifacts-shard-*.tar.zst.age` だけがfull artifactとして含まれ、暗号化前の `*.tar.zst` やraw CSV / JSONL / HTMLが残らない
- `Upload handoff package` のartifact名が `navii-detail-handoff-collector-navii-detail-YYYYMMDD-full-<github_run_id>` になる
- private prepare-load scheduleの前にrunが `success` で完了している

schedule失敗時の復旧:

1. failure logで、source 404、TLS、fetch error rate、age recipient、Actions timeout、shard単位失敗のどれかを確認する
2. source manifestの更新が必要ならPRで修正する
3. manual full runを実行してencrypted full artifactを再生成する
4. private prepare-load consumerを `collector_run_id` / `collector_artifact_name` 指定で再実行する

## MHLW Monthly Canary

MHLW monthlyは、まずcanary manifestの一部だけでdry-runします。

推奨input:

| input | value |
| --- | --- |
| `execute` | `false` |
| `confirm` | 空 |
| `source_manifest` | `sources/mhlw_monthly/source-manifest.json` |
| `run_label` | `collector-mhlw-monthly-202604-canary` |
| `artifact_mode` | `summary_only` |
| `max_sources` | `1` |
| `insecure_skip_tls_verify` | `false` |

確認:

- workflowがmanual dispatchでだけ起動する
- `mhlw-source-file-inventory.csv` が作られる
- `source-coverage-summary.csv` が作られる
- `SHA256SUMS` が作られる
- summary_onlyでraw ZIP/XLSXがuploadされていない

MHLW monthlyでraw source fileを取得する場合は、`execute=true`、`confirm=owner-approved-public-source-snapshot`、`artifact_mode=encrypted_full` にします。
厚労省側のTLS chain問題でsummary canaryが証明書検証のみ失敗する場合だけ、owner判断で `insecure_skip_tls_verify=true` を使います。

## MHLW Monthly Full Run

manual full runは、scheduleの補修、artifact再生成、manifest更新直後の確認に使います。canaryとowner local decrypt確認が済んでいない状態では実行しません。

推奨input:

| input | value |
| --- | --- |
| `execute` | `true` |
| `confirm` | `owner-approved-public-source-snapshot` |
| `source_snapshot_date` | 取得対象月の `YYYY-MM-01`。例: `2026-06-01` |
| `run_label` | `collector-mhlw-monthly-YYYYMM-full` |
| `artifact_mode` | `encrypted_full` |
| `workers` | `2` |
| `pause_seconds` | `0.5` |
| `max_sources` | `0` |

実行前確認:

- GitHub Actions minutesの見込み
- source側負荷
- encrypted artifact retention
- owner local decrypt作業時間
- private normalizer intake path

schedule runの期待値:

- `Resolve effective inputs` stepで `artifact_mode=encrypted_full`、`max_sources=0`、`run_label=collector-mhlw-monthly-YYYYMM-full` になっている
- `Package artifact` stepで `encrypted/raw-mhlw-source-files.tar.zst.age` だけがupload packageへ入り、暗号化前の `*.tar.zst` は残らない
- `Upload handoff package` のartifact名が `mhlw-monthly-handoff-collector-mhlw-monthly-YYYYMM-full-<github_run_id>` になる

schedule失敗時の復旧:

1. failure logで、source 404、TLS、checksum、age recipient、Actions timeoutのどれかを確認する
2. source manifestの更新が必要ならPRで修正する
3. manual full runを実行してencrypted full artifactを再生成する
4. private processing側pipelineで latest artifact resolutionを使い、対象pipelineのfetch smokeを確認する

## Handoff

private側へ渡す最小package:

- `manifest/source-snapshot-manifest.json`
- `manifest/collector-run-manifest.json`
- `metrics/fetch-metrics.json`
- `metrics/shard-summary.json`
- `metrics/coverage-summary.csv`
- `metrics/mhlw-source-file-inventory.csv` (MHLW monthly)
- `metrics/source-coverage-summary.csv` (MHLW monthly)
- `checksums/SHA256SUMS`
- `encrypted/raw-artifacts-shard-*.tar.zst.age`
- `encrypted/raw-mhlw-source-files.tar.zst.age` (MHLW monthly)
- GitHub run URL

private側へ渡さないもの:

- GitHub token
- owner private key
- private candidate CSV
- private processing result
- private datastore load script
- production secret

## Stop Conditions

- source manifestのURLが404 / 内容不一致
- `fetch_error_rate` がowner許容値を超える
- encrypted artifactが復号できない
- checksum不一致
- workflow logへraw full dataが出ている
- private consumer intakeがmanifestを読めない
