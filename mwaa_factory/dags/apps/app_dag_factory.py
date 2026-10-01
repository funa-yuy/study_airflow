"""
アプリケーション DAG 自動生成ファクトリ（Airflow 3.x Asset 対応）。
dags/apps/configs/*.yaml を読み込み、BashOperator によるタスクフローを自動生成します。
"""
from pathlib import Path
import yaml

from airflow import DAG
from airflow.models.param import Param
from airflow.sdk import Asset
from airflow.operators.bash import BashOperator

from common.default_args import COMMON_DEFAULT_ARGS

BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "configs"

if CONFIG_DIR.exists():
    for config_file in CONFIG_DIR.glob("*.yaml"):
        # テンプレートファイル（_ または example_ で始まるもの）は除外
        if config_file.name.startswith("_") or config_file.name.startswith("example_"):
            continue

        with open(config_file, "r", encoding="utf-8") as f:
            conf = yaml.safe_load(f)

        if not conf or not conf.get("name"):
            continue

        job_name = conf["name"]
        dag_id = f"dag_app_{job_name}"
        owner = conf.get("owner", "app-team")
        tags = conf.get("tags", ["app"])

        # 1. 起動スケジュールの設定（Cron指定があれば優先、なければ先行Assetトリガー）
        if conf.get("schedule"):
            schedule = conf.get("schedule")
        else:
            raw_schedule = [uri for uri in conf.get("schedule_assets", conf.get("schedule_datasets", [])) if uri]
            schedule = [Asset(uri) for uri in raw_schedule] if raw_schedule else None

        # 2. 完了通知の Asset 設定
        outlet_uri = conf.get("outlet_asset", conf.get("outlet_dataset"))
        outlets = [Asset(outlet_uri)] if outlet_uri else []

        # 3. UIの「Trigger DAG w/ config」用フォームの自動生成 (Params)
        dag_params = {
            "target_date": Param(
                default=None,
                type=["null", "string"],
                format="date",
                description="処理対象日付 (YYYY-MM-DD)。空欄時は論理実行日付(ds)を使用します",
            ),
            "force_run": Param(
                default=False,
                type="boolean",
                description="ONにすると先行条件等を無視して即時実行します",
            ),
        }

        tasks_conf = conf.get("tasks", {})
        for task_id, task_setting in tasks_conf.items():
            default_cmd = task_setting if isinstance(task_setting, str) else task_setting.get("command", "")
            dag_params[f"skip_{task_id}"] = Param(
                default=False,
                type="boolean",
                description=f"ONにすると一時的に {task_id} タスクをスキップします",
            )
            dag_params[f"{task_id}_command"] = Param(
                default=default_cmd,
                type=["null", "string"],
                description=f"{task_id} で実行するコマンド",
            )

        dag_default_args = {**COMMON_DEFAULT_ARGS, "owner": owner}

        with DAG(
            dag_id=dag_id,
            schedule=schedule,
            catchup=False,
            template_searchpath=[str(BASE_DIR)],
            default_args=dag_default_args,
            tags=tags,
            params=dag_params,
        ) as dag:

            prev_task = None
            task_items = list(tasks_conf.items())

            for i, (task_id, task_setting) in enumerate(task_items):
                default_cmd = task_setting if isinstance(task_setting, str) else task_setting.get("command", "")
                
                # 最後のタスクに完了通知Assetを付与
                is_last_task = (i == len(task_items) - 1)
                task_outlets = outlets if is_last_task else []

                current_task = BashOperator(
                    task_id=task_id,
                    bash_command=f"{{{{ params.get('{task_id}_command', '{default_cmd}') }}}}",
                    outlets=task_outlets,
                )

                if prev_task:
                    prev_task >> current_task
                prev_task = current_task

        globals()[dag_id] = dag
