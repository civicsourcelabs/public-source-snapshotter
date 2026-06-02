# Public Source Snapshotter

公開sourceから取得したsnapshot artifactを作るpublic collector repoです。非公開の後段処理、credential、deploy運用とは分離します。

このrepoはpublicです。secret、非公開処理用candidate、非公開処理結果、load script、復号private keyは置きません。

## 目的

public collector repoは、公開sourceから取得し、後段のprivate consumerへ渡せるartifactを作るだけを担当します。

扱うもの:

- 公開source manifest
- manual dispatch workflow
- fetch / shard / artifact packaging contract
- source collector implementation
- summary / metrics
- checksum
- encrypted full artifactの受け渡し形

扱わないもの:

- private processing strategy
- private datastore schemaやcandidate CSV
- private processing result
- private processing policy
- private datastore load script
- DB password、privileged DB credential、payment / email / deploy secret
- 復号private key

## 構成

```text
public-source-snapshotter/
  README.md
  .gitignore
  .github/
    workflows/
      navii-detail-snapshot.yml
      mhlw-monthly-source-snapshot.yml
  docs/
    artifact-schema.md
    collector-runbook.md
  keys/
    README.md
    owner-age-recipient.example.txt
  sources/
    navii/
      source-manifest.json
      README.md
    mhlw_monthly/
      source-manifest.json
      README.md
  collectors/
    navii_detail/
      README.md
      collect.py
    mhlw_monthly/
      README.md
      collect.py
```

## 実行の流れ

1. `source-manifest.json` のsnapshot date / URLをownerが確認する
2. `summary_only` canaryを実行する
3. `keys/owner-age-recipient.example.txt` を実際のage public recipient `keys/owner-age-recipient.txt` へ置き換える
4. `encrypted_full` canaryを実行し、owner localでdecrypt / checksum確認する
5. private consumer intakeで読めることを確認する
6. schedule化したcollectorが、private scheduleより前にencrypted full artifactを生成することを確認する

## 安全境界

- collectorはmanual canaryから始め、owner確認済みのものだけschedule化する
- MHLW monthly collectorは、private月次pipeline前にencrypted full artifactを作るscheduleを持つ
- Navii detail collectorは、全件取得負荷と利用判断が残るためmanual実行のまま扱う
- summary / metricsは平文でよいが、施設単位のfull artifactは暗号化する
- private keyはowner localだけに置く
- public repoのActionsからproduction DBへ接続しない
- public repoへprivate candidate、private processing result、load scriptを置かない
- private-side legacy workflowは、public collector canary完了まで削除しない
- empty repo bootstrap後のpublic repo変更は、branch + PRで行う
- `keys/owner-age-recipient.txt` に置くのはpublic recipientだけ。private keyはpublic repoにもGitHub secretにも置かない

## 関連docs

- `docs/artifact-schema.md`
- `docs/collector-runbook.md`
- `collectors/navii_detail/README.md`
- `collectors/mhlw_monthly/README.md`
- `sources/navii/source-manifest.json`
- `sources/mhlw_monthly/source-manifest.json`
