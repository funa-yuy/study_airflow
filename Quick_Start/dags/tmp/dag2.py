
import textwrap
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import DAG, Dataset
with DAG(
    "tmp_dag2", # dag_id
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG同士の依存関係の検証",
    schedule=[Dataset("tmp_dag1_completed")],  # Datasetが更新されたら自動起動
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["テスト"],
) as dag:
    templated_command = textwrap.dedent(
        """
        echo "DAG2です。"
    """
    )

    t1 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )


    t3 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t1 >> [t2, t3]