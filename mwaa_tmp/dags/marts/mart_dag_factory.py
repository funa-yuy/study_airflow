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
