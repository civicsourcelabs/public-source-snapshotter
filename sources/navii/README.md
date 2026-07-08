# Navii Source Manifest

`source-manifest.json` は、ナビィ / 医療情報ネットの公式open data snapshotを解決するためのtemplateです。

このmanifestには公式open data page、取得対象kind、公式link label、期待filenameだけを置きます。固定のsnapshot dateやdated URLは置かず、workflowが公式pageから解決します。

更新時は次を確認します。

- `source_snapshot_date` が `auto` であること
- dated URLがmanifestに固定されていないこと
- 公式page上のlink labelと一致していること
- expected filename
- checksumはworkflow実行時にartifact側で生成すること
- source terms URL
