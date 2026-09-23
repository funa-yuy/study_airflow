"""
ECS Fargate タスクを実行するアプリケーションDAGの標準テンプレート。
アプリケーションチームは必要に応じてこのファイルをコピー・編集して使用してください。
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from common.constants import DEFAULT_REDSHIFT_CONN_ID
from common.default_args import COMMON_DEFAULT_ARGS

with DAG(
    dag_id="dag_app_sample_ecs_job",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={**COMMON_DEFAULT_ARGS, "owner": "app-team-a"},
    tags=["app", "ecs", "team-a"],
) as dag:

    run_ecs_task = EcsRunTaskOperator(
        task_id="run_batch_container",
        cluster="prod-app-cluster",
        task_definition="my-batch-job:latest",
        launch_type="FARGATE",
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ["subnet-xxxxxx"],
                "securityGroups": ["sg-xxxxxx"],
                "assignPublicIp": "DISABLED",
            }
        },
        aws_conn_id="aws_default",
    )
