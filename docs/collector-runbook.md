# Collector Runbook

このpublic repoでsource snapshotを取得するためのrunbookです。

このrunbookはpublic collector側だけを扱います。downstream candidate出力、consumer inventory、policy反映、datastore loadは扱いません。

## Supported collectors

| collector | workflow | role |
| --- | --- | --- |
| Navii detail | `Source Snapshot: Navii Detail` | ナビィopen dataからdetail pageを取得し、全sectionのtable row / table linkとsource-level coverageを抽出する |
| MHLW monthly | `Source Snapshot: MHLW Monthly` | 厚労省地方厚生局の月次source ZIP/XLSXを取得し、checksum付きraw source packageを作る |

## Schedule policy

MHLW monthly collectorとNavii detail collectorは、後続consumerが使う前にencrypted full artifactを生成します。

| workflow | schedule | effective config |
| --- | --- | --- |
| `Source Snapshot: MHLW Monthly` | 毎月5日 07:00 JST / 毎月8日 07:00 JST | `execute=true`、`artifact_mode=encrypted_full`、`max_sources=0`、`source_manifest=sources/mhlw_monthly/source-manifest.json` |
| `Source Snapshot: Navii Detail` | 毎月5日 00:30 JST | `execute=true`、`artifact_mode=encrypted_full`、`candidate_mode=all`、`shard_count=16`、`max_parallel=16`、`max_pages_per_shard=0`、`artifact_retention_days=14` |

MHLW monthly runでは、schedule / manualともにAsia/Tokyoの実行月から `YYYY-MM-01` の `source_snapshot_date` とrun labelを自動生成します。後続consumerは最新成功full artifactを解決して使うため、public repo側の5日runが失敗した場合は8日runで再生成します。

Navii detail runでは、Asia/Tokyoの実行月から `YYYY-MM-01` の期待snapshot dateを自動生成します。公式open data pageに該当snapshotと必須linkがなければfail closedし、利用可能な最新snapshotや前回snapshotへfallbackしません。run labelも `collector-navii-detail-YYYYMMDD-{canary|scope|full}` として自動生成します。scheduleは毎月5日にreadiness pollとして動きますが、同じsnapshot dateのfull artifactが既に成功していれば後続jobをskipします。

## 初回セットアップ

完了済み:

- ownerがpublic repoを作成する
- scaffoldをrepo rootへ転記し、初回empty repo bootstrapを `main` へpushする
- collector実装 `collectors/navii_detail/collect.py` を追加する

未完了:

1. `sources/navii/source-manifest.json` から公式page readinessを解決できることを確認する
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
| `artifact_mode` | `summary_only` |
| `shard_count` | `1` |
| `max_parallel` | `1` |
| `max_pages_per_shard` | `400` |

確認:

- workflowがmanual dispatchでだけ起動する
- `Validate inputs` stepでsource snapshot dateとrun labelが自動生成される
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
| `candidate_mode` | `sample` |
| `sample_per_kind` | `25` |
| `sample_strategy` | `prefecture-stratified` |
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
- consumer intakeに渡せる
- 暗号化前の `*.tar.zst` やraw CSV / JSONL / HTMLがGitHub artifactへ残っていない
- 復号済みartifactに `table-rows.csv.gz`、`links.csv.gz`、`phone-numbers.csv.gz` がある
- `coverage-summary.csv.gz` に `target_group=all_detail` がある

TLS certificate verificationだけが失敗する場合に限り、owner判断で `insecure_skip_tls_verify=true` を使います。通常runでは `false` のままにします。

## Navii Smoke: encrypted full around 100 pages

目的は、全section table/link artifactを少数ページで確認し、full run前にschema、暗号化、復号、row countを確認することです。

推奨input:

| input | value |
| --- | --- |
| `execute` | `true` |
| `confirm` | `owner-approved-public-source-snapshot` |
| `artifact_mode` | `encrypted_full` |
| `kinds` | `hospital,clinic,dental,pharmacy` |
| `candidate_mode` | `sample` |
| `sample_per_kind` | `25` |
| `sample_strategy` | `prefecture-stratified` |
| `shard_count` | `1` |
| `shard_indexes` | `0` |
| `max_parallel` | `1` |
| `workers` | `2` |
| `pause_seconds` | `0.3` |
| `jitter_seconds` | `0.1` |
| `max_pages_per_shard` | `100` |
| `artifact_retention_days` | `7` |

確認:

- `collector-run-manifest.json` の `candidate_selection.mode` が `sample`
- `fetch-metrics.json` の `candidate_count` が約100
- encrypted shardが1件だけ作られている
- 復号済みartifactに `table-rows.csv.gz`、`links.csv.gz`、`phone-numbers.csv.gz`、`run-metrics.json` がある
- `run-metrics.json` に `table_rows` と `link_rows` が記録されている
- `coverage-summary.csv.gz` に `target_group=all_detail` がある

## Navii Detail Full Run

manual full runは、schedule失敗時の再生成、manifest更新直後の確認、またはconsumer artifactを固定したい場合に使います。canaryとowner local decrypt確認が済んでいない状態では実行しません。

推奨input:

| input | value |
| --- | --- |
| `execute` | `true` |
| `confirm` | `owner-approved-public-source-snapshot` |
| `artifact_mode` | `encrypted_full` |
| `candidate_mode` | `all` |
| `sample_per_kind` | `25` |
| `sample_strategy` | `prefecture-stratified` |
| `shard_count` | `16` |
| `max_parallel` | `16` |
| `workers` | `2` |
| `pause_seconds` | `0.3` |
| `jitter_seconds` | `0.1` |
| `max_pages_per_shard` | `0` |
| `artifact_retention_days` | `14` |

schedule runの期待値:

- `Validate inputs` stepで `execute=true`、`artifact_mode=encrypted_full`、`shard_count=16`、`max_pages_per_shard=0` になっている
- `Validate inputs` stepで公式open data pageから期待snapshot dateを解決し、`run_label=collector-navii-detail-YYYYMMDD-full` になっている
- `Package handoff artifact` stepで `encrypted/raw-artifacts-shard-*.tar.zst.age` だけがfull artifactとして含まれ、暗号化前の `*.tar.zst` やraw CSV / JSONL / HTMLが残らない
- `Upload handoff package` のartifact名が `navii-detail-handoff-collector-navii-detail-YYYYMMDD-full-<github_run_id>` になる
- consumer scheduleの前にrunが `success` で完了している

schedule失敗時の復旧:

1. failure logで、source 404、TLS、fetch error rate、age recipient、Actions timeout、shard単位失敗のどれかを確認する
2. source manifestの更新が必要ならPRで修正する
3. manual full runを実行してencrypted full artifactを再生成する
4. 後続consumerを対象run / artifact指定で再実行する

## MHLW Monthly Canary

MHLW monthlyは、まずcanary manifestの一部だけでdry-runします。

推奨input:

| input | value |
| --- | --- |
| `execute` | `false` |
| `confirm` | 空 |
| `source_manifest` | `sources/mhlw_monthly/source-manifest.json` |
| `run_label` | 入力しない。canary条件では `collector-mhlw-monthly-YYYYMM-canary` を自動生成 |
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
| `source_snapshot_date` | 入力しない。Asia/Tokyoの実行月から自動生成 |
| `run_label` | 入力しない。full条件では `collector-mhlw-monthly-YYYYMM-full` を自動生成 |
| `artifact_mode` | `encrypted_full` |
| `workers` | `2` |
| `pause_seconds` | `0.5` |
| `max_sources` | `0` |

実行前確認:

- GitHub Actions minutesの見込み
- source側負荷
- encrypted artifact retention
- owner local decrypt作業時間
- consumer intake path

schedule runの期待値:

- `Resolve effective inputs` stepで `artifact_mode=encrypted_full`、`max_sources=0`、`run_label=collector-mhlw-monthly-YYYYMM-full` になっている
- `Package artifact` stepで `encrypted/raw-mhlw-source-files.tar.zst.age` だけがupload packageへ入り、暗号化前の `*.tar.zst` は残らない
- `Upload handoff package` のartifact名が `mhlw-monthly-handoff-collector-mhlw-monthly-YYYYMM-full-<github_run_id>` になる

schedule失敗時の復旧:

1. failure logで、source 404、TLS、checksum、age recipient、Actions timeoutのどれかを確認する
2. source manifestの更新が必要ならPRで修正する
3. manual full runを実行してencrypted full artifactを再生成する
4. consumer pipelineで latest artifact resolutionを使い、対象pipelineのfetch smokeを確認する

## Handoff

後続consumerへ渡す最小package:

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

artifactに含めないもの:

- GitHub token
- owner private key
- downstream candidate CSV
- downstream processing result
- datastore load script
- production secret

## Stop Conditions

- source manifestのURLが404 / 内容不一致
- `fetch_error_rate` がowner許容値を超える
- encrypted artifactが復号できない
- checksum不一致
- workflow logへraw full dataが出ている
- consumer intakeがmanifestを読めない
