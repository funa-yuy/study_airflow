"""
マート作成 DAG 自動生成ファクトリ（Airflow 3.x Asset 対応）

スキャン対象:
  dags/marts/<product>/*.yaml  （_始まりは除外）

各yamlが1つのDAGに対応する。タスクは drop → create → unload の固定順。
"""
import sys
from pathlib import Path

# dags ディレクトリを sys.path に追加して common などのモジュールを import 可能にする
DAGS_DIR = Path(__file__).resolve().parents[2]  # dags/
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

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

FACTORY_DIR = Path(__file__).parent   # dags/marts/_factory/
MARTS_DIR   = FACTORY_DIR.parent      # dags/marts/


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

TASK_BUILDERS = {
    "drop":   build_drop_task,
    "create": build_create_task,
    "unload": build_unload_task,
    "bash":   build_bash_task,
}

# =============================================================
# メインループ: marts/<product>/*.yaml を検索して DAG を生成
# =============================================================
for config_file in sorted(MARTS_DIR.glob("*/*.yaml")):
    # _始まり・example_始まりは除外
    if config_file.name.startswith("_") or config_file.name.startswith("example_"):
        continue

    with open(config_file, "r", encoding="utf-8") as f:
        conf = yaml.safe_load(f)

    if not conf or not conf.get("name"):
        continue

    job_name = conf["name"]
    dag_id   = f"dag_mart_{job_name}"
    owner    = conf.get("owner", "mart-team")
    tags     = conf.get("tags", ["mart"])

    # 接続設定（yamlに個別指定があれば優先、なければ共通デフォルト）
    database      = conf.get("database",      DEFAULT_REDSHIFT_DATABASE)
    workgroup_name = conf.get("workgroup_name", DEFAULT_REDSHIFT_WORKGROUP)
    aws_conn_id   = conf.get("aws_conn_id",   DEFAULT_REDSHIFT_CONN_ID)

    # 起動トリガー Asset 設定
    raw_schedule = [uri for uri in conf.get("schedule_assets", conf.get("schedule_datasets", [])) if uri]
    schedule = [Asset(uri) for uri in raw_schedule] if raw_schedule else None

    # 完了通知 Asset 設定
    outlet_uri     = conf.get("outlet_asset", conf.get("outlet_dataset"))
    create_outlets = [Asset(outlet_uri)] if outlet_uri else []

    dag_default_args = {**COMMON_DEFAULT_ARGS, "owner": owner}

    context = {
        "database":       database,
        "workgroup_name": workgroup_name,
        "aws_conn_id":    aws_conn_id,
        "create_outlets": create_outlets,
    }

    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        catchup=False,
        template_searchpath=[str(MARTS_DIR)],
        default_args=dag_default_args,
        tags=tags,
    ) as dag:

        tasks_conf = conf.get("tasks", {})
        prev_task  = None

        for task_id, task_setting in tasks_conf.items():
            builder = TASK_BUILDERS.get(task_id)
            if not builder:
                raise ValueError(
                    f"[{dag_id}] 未知のタスクID '{task_id}' が指定されています。"
                    f"有効なタスクID: {list(TASK_BUILDERS.keys())}"
                )

            task_dict    = {"path": task_setting} if isinstance(task_setting, str) else task_setting
            current_task = builder(task_id, task_dict, context)

            if prev_task:
                prev_task >> current_task
            prev_task = current_task

    globals()[dag_id] = dag
