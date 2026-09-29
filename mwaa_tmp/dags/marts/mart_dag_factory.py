"""
マート作成 DAG 自動生成ファクトリ（Airflow 3.x Asset 対応）。
dags/marts/configs/*.yaml を読み込み、指定されたタスクを順次結合した DAG を自動生成します。
"""
from pathlib import Path
import yaml

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.bash import BashOperator

from common.constants import (
    DEFAULT_REDSHIFT_CONN_ID,
    DEFAULT_REDSHIFT_DATABASE,
    DEFAULT_REDSHIFT_WORKGROUP,
)
from common.default_args import COMMON_DEFAULT_ARGS

BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "configs"

# =============================================================
# タスク生成関数（ビルダー）の定義
# =============================================================
def build_drop_task(task_id: str, task_conf: dict, context: dict):
    return RedshiftDataOperator(
        task_id=task_id,
        sql=task_conf.get("path"),
        database=context["database"],
        workgroup_name=context["workgroup_name"],
        aws_conn_id=context["aws_conn_id"],
    )

def build_create_task(task_id: str, task_conf: dict, context: dict):
    return RedshiftDataOperator(
        task_id=task_id,
        sql=task_conf.get("path"),
        database=context["database"],
        workgroup_name=context["workgroup_name"],
        aws_conn_id=context["aws_conn_id"],
        outlets=context["create_outlets"],
    )

def build_unload_task(task_id: str, task_conf: dict, context: dict):
    return RedshiftDataOperator(
        task_id=task_id,
        sql=task_conf.get("path"),
        database=context["database"],
        workgroup_name=context["workgroup_name"],
        aws_conn_id=context["aws_conn_id"],
    )

def build_bash_task(task_id: str, task_conf: dict, context: dict):
    return BashOperator(
        task_id=task_id,
        bash_command=task_conf.get("path"),
    )

# タスクIDとビルダー関数のマッピング辞書
TASK_BUILDERS = {
    "drop": build_drop_task,
    "create": build_create_task,
    "unload": build_unload_task,
    "bash": build_bash_task,
}

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

    context = {
        "database": database,
        "workgroup_name": workgroup_name,
        "aws_conn_id": aws_conn_id,
        "create_outlets": create_outlets,
    }

    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        catchup=False,
        template_searchpath=[str(BASE_DIR)],
        default_args=dag_default_args,
        tags=tags,
    ) as dag:

        tasks_conf = conf.get("tasks", {})
        prev_task = None

        for task_id, task_setting in tasks_conf.items():
            builder = TASK_BUILDERS.get(task_id)
            if not builder:
                raise ValueError(f"[{dag_id}] 未知のタスクID '{task_id}' が指定されています。有効なタスクID: {list(TASK_BUILDERS.keys())}")

            # 文字列直指定にも辞書形式（path: ...）にも対応
            task_dict = {"path": task_setting} if isinstance(task_setting, str) else task_setting
            current_task = builder(task_id, task_dict, context)

            if prev_task:
                prev_task >> current_task
            prev_task = current_task

    globals()[dag_id] = dag
