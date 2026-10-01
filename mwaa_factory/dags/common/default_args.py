"""
全DAG共通の default_args 設定。
"""
from datetime import timedelta


def default_failure_callback(context):
    """タスク失敗時の共通アラート処理（Slack/SNS等への通知）"""
    task_instance = context.get("task_instance")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    print(f"[ALERT] Task Failed: {dag_id}.{task_id}")


COMMON_DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": default_failure_callback,
}
