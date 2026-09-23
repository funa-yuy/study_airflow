"""
EC2 上のスクリプトを AWS Systems Manager (SSM) で実行するDAGテンプレート。
既存EC2内のバッチ処理を移行する際に使用してください。
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.ssm import SsmOperator

from common.default_args import COMMON_DEFAULT_ARGS

with DAG(
    dag_id="dag_app_sample_ssm_job",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={**COMMON_DEFAULT_ARGS, "owner": "app-team-b"},
    tags=["app", "ssm", "legacy-migration"],
) as dag:

    execute_ec2_script = SsmOperator(
        task_id="exec_script_on_ec2",
        instance_ids=["i-0123456789abcdef0"],
        document_name="AWS-RunShellScript",
        parameters={"commands": ["python3 /opt/scripts/my_job.py"]},
        aws_conn_id="aws_default",
    )
