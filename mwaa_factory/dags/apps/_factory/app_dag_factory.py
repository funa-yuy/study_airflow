"""
アプリケーション DAG 自動生成ファクトリ（Airflow 3.x 対応）

スキャン対象:
  - DAG定義  : dags/apps/product/<product>/dag_*.yaml
  - タスク定義: dags/apps/product/<product>/tasks/*.yaml

タスクの依存関係は各 task.yaml の depends_on.tasks で定義する。

タスクの指定方式（いずれかを選択）:
  - operator: "bash"（推奨）: command に書かれたシェルコマンドを直接実行
  - type: "ecs_run"（互換） : apps/scripts/ecs_run.sh を自動組み立てて実行

TODO:
  現在は 'operator'と'type'が混在しているため、今後は 'operator'（例: operator: bash, operator: ecs_run）に
  一本化したい。ecs_run.shをカスタムOperator化したい
"""
import sys
from pathlib import Path

# dags ディレクトリを sys.path に追加して common などのモジュールを import 可能にする
DAGS_DIR = Path(__file__).resolve().parents[2]  # dags/
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

import yaml

from airflow import DAG
from airflow.models.param import Param
from airflow.sdk import Asset
from airflow.operators.bash import BashOperator

from common.default_args import COMMON_DEFAULT_ARGS

FACTORY_DIR = Path(__file__).parent   # dags/apps/_factory/
APPS_DIR    = FACTORY_DIR.parent      # dags/apps/
SCRIPTS_DIR = APPS_DIR / "scripts"
PRODUCT_PARENT_DIR = APPS_DIR / "product"  # dags/apps/product/


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_bash_command(tc: dict, product_dir: Path) -> str:
    """タスク設定からBashOperatorに渡すコマンド文字列を生成する。"""
    # 1. command が直接指定されている場合はそれを最優先
    if "command" in tc:
        return tc["command"]

    # 2. type によるコマンド自動組み立て（ecs_run など）
    task_type = tc.get("type", "bash")

    if task_type == "ecs_run":
        ecs_conf = tc.get("ecs_run", {})
        script  = ecs_conf.get("script", "ecs_run.sh")
        config  = ecs_conf.get("config", "")
        config_path = product_dir / config if config else ""
        return f"bash {SCRIPTS_DIR}/{script} {config_path}"

    # bash タイプ（デフォルト）
    return tc.get("command", f"echo '[{tc.get('task_id', 'unknown')}] No command defined.'")


# =============================================================
# メインループ: apps/product/<product>/dag_*.yaml を検索して DAG を生成
# =============================================================
for dag_yaml_path in sorted(PRODUCT_PARENT_DIR.glob("*/dag_*.yaml")):
    product_dir  = dag_yaml_path.parent
    product_name = product_dir.name

    dag_conf = _load_yaml(dag_yaml_path)
    if not dag_conf.get("dag_id"):
        continue

    dag_id = f"dag_app_{dag_conf['dag_id']}"
    owner  = dag_conf.get("owner", "app-team")
    tags   = dag_conf.get("tags", ["app"])

    # ---- スケジュール設定 ----
    if dag_conf.get("schedule"):
        schedule = dag_conf["schedule"]
    else:
        raw_assets = [uri for uri in dag_conf.get("schedule_assets", []) if uri]
        schedule   = [Asset(uri) for uri in raw_assets] if raw_assets else None

    # ---- 完了通知 Asset ----
    outlet_uri = dag_conf.get("outlet_asset")
    outlets    = [Asset(outlet_uri)] if outlet_uri else []

    # ---- タスク yaml を全て読み込む ----
    tasks_dir   = product_dir / "tasks"
    task_configs: dict[str, dict] = {}

    if tasks_dir.exists():
        for task_yaml_path in sorted(tasks_dir.glob("*.yaml")):
            if task_yaml_path.name.startswith("_"):
                continue
            tc = _load_yaml(task_yaml_path)
            if not tc.get("task_id"):
                continue
            # dag_id が一致するタスクのみ登録（他DAGのタスクが混入しないよう防護）
            if tc.get("dag_id") != dag_conf["dag_id"]:
                continue
            task_configs[tc["task_id"]] = tc

    if not task_configs:
        continue

    # ---- ターミナルタスク（後続タスクを持たないもの）を特定 ----
    # → outlet_asset を付与するタスクを自動判定する
    has_upstream_ref: set[str] = set()
    for tc in task_configs.values():
        has_upstream_ref.update(tc.get("depends_on", {}).get("tasks", []))
    terminal_task_ids = set(task_configs.keys()) - has_upstream_ref

    # ---- Params ----
    dag_params = {
        "target_date": Param(
            default=None,
            type=["null", "string"],
            format="date",
            description="処理対象日付 (YYYY-MM-DD)。空欄時は論理実行日付(ds)を使用",
        ),
        "force_run": Param(
            default=False,
            type="boolean",
            description="ONにすると先行条件を無視して即時実行",
        ),
    }

    dag_default_args = {**COMMON_DEFAULT_ARGS, "owner": owner}

    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        catchup=False,
        default_args=dag_default_args,
        tags=tags,
        params=dag_params,
    ) as dag:

        task_objects: dict[str, BashOperator] = {}

        # ---- タスクオブジェクト生成 ----
        for task_id, tc in task_configs.items():
            bash_cmd     = _build_bash_command(tc, product_dir)
            task_outlets = outlets if task_id in terminal_task_ids else []
            operator_type = tc.get("operator", "bash")

            if operator_type == "bash":
                task_objects[task_id] = BashOperator(
                    task_id=task_id,
                    bash_command=bash_cmd,
                    cwd=str(DAGS_DIR),
                    outlets=task_outlets,
                )
            else:
                raise ValueError(f"Unsupported operator: '{operator_type}' for task '{task_id}'")

        # ---- depends_on に基づいて依存関係を設定 ----
        for task_id, tc in task_configs.items():
            upstream_ids = tc.get("depends_on", {}).get("tasks", [])
            for upstream_id in upstream_ids:
                if upstream_id in task_objects:
                    task_objects[upstream_id] >> task_objects[task_id]

    globals()[dag_id] = dag
