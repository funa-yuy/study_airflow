# Redshiftテーブル作成DAGアーキテクチャ設計仕様書（Airflow 3.x Asset仕様）

## 1. 概要

Redshiftのマートテーブル作成プロセス（`drop` → `create` → `unload`）を管理する仕組み。
DAG間の依存関係制御には **Airflow 3.x の Assets（Data-aware Scheduling）** を使用し、MWAAのリソース消費を抑えた疎結合な連携とする。

マート作成チームは **SQLファイルのみを作成** し、YAML設定ファイル（宣言的設定）を配置するだけでDAGが自動生成されるアーキテクチャとする。

---

## 2. 設計制約・原則

1. **DAG単位**: 1テーブル＝1DAG（成果物単位）で管理する。
2. **タスク構造とIDの固定化（確認済み）**: タスクの実行順序（`drop` → `create` → `unload`）およびタスクIDは固定とする。ただし `unload` は一部テーブルのみで使用するためオプション（任意）とする。
3. **Asset URI指定**: スキーマやクラスター、外部データソース（S3等）の不整合や固定化を防ぐため、入力トリガー（`schedule_assets`）および出力通知（`outlet_asset`）はYAML内でURI形式で明示する。
4. **起動・スケジュール方式（確認済み）**: 全マートが「日次バッチかつ前段データ完了トリガー（Asset連鎖）」で稼働する。最上流テーブルは、外部WFの前段create完了を通知する起点DAGから発行される Asset をトリガーとして起動する。
5. **接続設定のデフォルト化と上書き**: 社内標準の接続先（`analytics` DB / `prod-workgroup`）を共通デフォルトとして適用しつつ、特殊なテーブルのみYAMLで個別上書き可能とする。
6. **ガバナンス管理**: SQLとDAG設定は同じGitリポジトリ（モノリポジトリ）で管理し、CI（`pytest`）で `ExternalTaskSensor` の使用禁止ルール等を検知・ガードする。

---

## 3. ディレクトリ構成

```text
dags/
├── common/                          # 【社内共通】全チーム共通モジュール
│   ├── constants.py                 # デフォルト接続情報（Redshift/AWS）
│   └── default_args.py              # 共通 default_args (retries, アラート通知等)
│
├── marts/                           # 【マートチーム管理領域】
│   ├── configs/
│   │   ├── _template.yaml           # 配布用空テンプレート（複製用）
│   │   └── user_summary.yaml        # テーブル作成設定の実例
│   ├── sqls/
│   │   └── user_summary/
│   │       ├── drop.sql             # DROP TABLE SQL
│   │       ├── create.sql           # CREATE TABLE AS SELECT SQL
│   │       └── unload.sql           # UNLOAD SQL (任意)
│   └── mart_dag_factory.py          # YAMLからDAGを自動生成するファクトリ
│
└── apps/                            # 【アプリケーションチーム管理領域】
    └── templates/                   # 各種Operatorの参考テンプレート
```

---

## 4. マートチーム向け利用手順

マート作成者は以下の3ステップのみで新しいマートDAGをデプロイできます。

### ステップ1: SQLファイルの作成
`dags/marts/sqls/<テーブル名>/` ディレクトリを作成し、必要なSQLファイルを配置します。
- `drop.sql`: 既存テーブルの削除
- `create.sql`: テーブルの作成・データ挿入（`CREATE TABLE ... AS SELECT ...`）
- `unload.sql`: （必要な場合のみ）S3へのエクスポート処理

### ステップ2: 設定ファイルの作成
`dags/marts/configs/_template.yaml` をコピーして `dags/marts/configs/<テーブル名>.yaml` を作成し、必要項目を記入します。

### ステップ3: Gitコミット＆プッシュ
CIでバリデーションが実行され、通過するとMWAAに自動反映されてDAGが登録されます。

---

## 5. 実装コード・設定ファイル詳細

### ① 配布用テンプレート（`dags/marts/configs/_template.yaml`）

```yaml
# ============================================================
# 【マート作成設定テンプレート】
# 新規テーブル作成時はこのファイルをコピーし、<テーブル名>.yaml として保存してください。
# ============================================================

# 1. 基本情報（必須）
name: ""                        # テーブル名 / ジョブ名（DAG ID: dag_mart_<name> になります）
owner: ""                       # 担当者またはチーム名
tags:                           # Airflow UIで検索・分類するためのタグ
  - "mart"

# 2. 起動トリガー（前段の Asset URI 一覧）
# ※ 前段テーブルの Asset URI を指定します。
# ※ 起点テーブルの場合は、起点通知Asset（例: "system://workflow_sync/daily_raw_data_ready"）を指定します。
schedule_assets:
  - ""

# 3. 完了通知（自身が発行する Asset URI）
# ※ 後続DAGに更新を通知する URI を指定します（後続がない場合は空文字または省略可）。
outlet_asset: ""

# 4. 実行タスク定義（sqls/ からの相対パス）
tasks:
  drop: ""                      # DROP TABLE 用の SQL ファイルパス
  create: ""                    # CREATE TABLE AS SELECT 用の SQL ファイルパス
  unload: ""                    # UNLOAD 用の SQL ファイルパス（不要な場合は空文字または行削除）

# 5. 接続先オーバーライド（任意）
# ※ デフォルト以外のデータベースやワークグループに接続する場合のみコメントアウトを解除して指定
# database: ""
# workgroup_name: ""
# aws_conn_id: ""
```

---

### ② 設定実例（`dags/marts/configs/user_summary.yaml`）

```yaml
# ============================================================
# 【設定実例】user_summary テーブル作成設定
# ============================================================

name: "user_summary"
owner: "mart-team"
tags:
  - "mart"
  - "user_analytics"

# 前段の生データ（raw_users, raw_events）の更新完了を検知して起動
schedule_assets:
  - "redshift://prod_cluster/raw_schema/raw_users"
  - "redshift://prod_cluster/raw_schema/raw_events"

# create完了時に後続DAGへ更新を通知するAsset
outlet_asset: "redshift://prod_cluster/mart_schema/user_summary"

# 各タスクで実行するSQLファイルの指定
tasks:
  drop: "sqls/user_summary/drop.sql"
  create: "sqls/user_summary/create.sql"
  unload: "sqls/user_summary/unload.sql"
```

---

### ③ 共通ファクトリ処理（`dags/marts/mart_dag_factory.py`）

```python
"""
マート作成 DAG 自動生成ファクトリ（Airflow 3.x Asset 対応）。
dags/marts/configs/*.yaml を読み込み、drop -> create -> unload の固定フローを持つ DAG を自動生成します。
"""
from pathlib import Path
import yaml

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

from common.constants import (
    DEFAULT_REDSHIFT_CONN_ID,
    DEFAULT_REDSHIFT_DATABASE,
    DEFAULT_REDSHIFT_WORKGROUP,
)
from common.default_args import COMMON_DEFAULT_ARGS

BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "configs"

for config_file in CONFIG_DIR.glob("*.yaml"):
    # テンプレートファイル（_ または example_ で始まるもの）は除外
    if config_file.name.startswith("_") or config_file.name.startswith("example_"):
        continue

    with open(config_file, "r", encoding="utf-8") as f:
        conf = yaml.safe_load(f)

    if not conf or not conf.get("name"):
        continue

    job_name = conf["name"]
    dag_id = f"dag_mart_{job_name}"
    owner = conf.get("owner", "mart-team")
    tags = conf.get("tags", ["mart"])

    # 接続設定（YAMLに個別指定があれば優先、なければ共通デフォルト）
    database = conf.get("database", DEFAULT_REDSHIFT_DATABASE)
    workgroup_name = conf.get("workgroup_name", DEFAULT_REDSHIFT_WORKGROUP)
    aws_conn_id = conf.get("aws_conn_id", DEFAULT_REDSHIFT_CONN_ID)

    # 1. 起動トリガーの Asset 設定
    raw_schedule = [uri for uri in conf.get("schedule_assets", conf.get("schedule_datasets", [])) if uri]
    schedule = [Asset(uri) for uri in raw_schedule] if raw_schedule else None

    # 2. 完了通知の Asset 設定
    outlet_uri = conf.get("outlet_asset", conf.get("outlet_dataset"))
    create_outlets = [Asset(outlet_uri)] if outlet_uri else []

    # default_args のマージ
    dag_default_args = {**COMMON_DEFAULT_ARGS, "owner": owner}

    tasks_conf = conf.get("tasks", {})

    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        catchup=False,
        template_searchpath=[str(BASE_DIR)],
        default_args=dag_default_args,
        tags=tags,
    ) as dag:

        # 1. Drop タスク (固定ID: drop)
        drop_sql_path = tasks_conf.get("drop")
        if not drop_sql_path:
            raise ValueError(f"[{dag_id}] tasks.drop のパスが指定されていません。")

        drop_task = RedshiftDataOperator(
            task_id="drop",
            sql=drop_sql_path,
            database=database,
            workgroup_name=workgroup_name,
            aws_conn_id=aws_conn_id,
        )

        # 2. Create タスク (固定ID: create / 完了時にoutlet_assetを発行)
        create_sql_path = tasks_conf.get("create")
        if not create_sql_path:
            raise ValueError(f"[{dag_id}] tasks.create のパスが指定されていません。")

        create_task = RedshiftDataOperator(
            task_id="create",
            sql=create_sql_path,
            database=database,
            workgroup_name=workgroup_name,
            aws_conn_id=aws_conn_id,
            outlets=create_outlets,
        )

        drop_task >> create_task

        # 3. Unload タスク (固定ID: unload / 指定がある場合のみ生成)
        unload_sql_path = tasks_conf.get("unload")
        if unload_sql_path:
            unload_task = RedshiftDataOperator(
                task_id="unload",
                sql=unload_sql_path,
                database=database,
                workgroup_name=workgroup_name,
                aws_conn_id=aws_conn_id,
            )
            create_task >> unload_task

    globals()[dag_id] = dag
```