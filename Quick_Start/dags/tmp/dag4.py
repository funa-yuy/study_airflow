
import textwrap
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from airflow.sdk import DAG
with DAG(
    "tmp_dag4", # dag_id
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG同士の依存関係の検証",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["テスト"],
) as dag:
    templated_command = textwrap.dedent(
        """
        echo "DAG4です。"
    """
    )
    wait_for_dag1 = ExternalTaskSensor(
       task_id="wait_for_dag1",
       external_dag_id="tmp_dag3",
       external_task_id=None,  # DAG全体が完了するのを待機
       mode="reschedule",
       poke_interval=60,
       timeout=3600,
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

    wait_for_dag1 >> t1 >> [t2, t3]