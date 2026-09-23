# アプリケーションDAG設計方針 兼 スクリプト実行設計仕様書（Airflow 3.x対応）

## 1. 概要と位置づけ

MWAA環境において、マート作成DAG（宣言的YAML＋固定フロー）と並行して稼働する 一般的なアプリケーション処理・バッチ処理用DAG の設計方針です。

アプリケーションDAGは、各開発チームが担う多種多様な処理（ECS Fargate上のコンテナ起動、EC2上の既存スクリプト実行、AWS Lambda/Glue呼び出しなど）を実行します。

---

## 2. マートDAGとの共存・運用方針

1. ディレクトリと作成方式の分離:
   - マートDAG: `dags/marts/` 配下で YAML設定＋SQL による自動生成（Factory）で運用。
   - アプリケーションDAG: `dags/apps/` 配下で Python DAG を直接記述 して自由なタスクフローを構築。
2. 共通モジュール（`dags/common/`）の活用:
   - 全アプリ共通で社内標準の `COMMON_DEFAULT_ARGS`（リトライ設定、エラー時の共通Slack通知など）を継承して利用する。
   - 独自の通知先やリトライ回数を設定したい場合は、個別DAG側で上書き可能。
3. Asset連携（マート ↔ アプリ）:
   - アプリケーションDAGがS3等にデータを出力した際、`outlets=[Asset("...")]` を指定することで、後続のマート作成DAGを自動トリガー可能。

---

## 3. スクリプト実行アプローチの比較評価

| 記号 | アプローチ | 実行基盤 | 移行コスト | インフラコスト | 運用・コード分離 | 概要・推奨用途 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 (推奨) | ECS Fargate 実行 | ECS (Fargate) | 〇 中 | ◎ 極小 | ◎ 完全分離 | 【新規・標準推奨】コンテナとしてタスクを実行。MWAAワーカーに負荷をかけず環境も完全分離。 |
| 2 | EC2一時起動・停止 ＋ SSM実行 | 既存EC2 | 〇 低〜中 | ◎ 極小 | 〇 分離 | 【移行推奨】実行直前にEC2を起動しSSMで実行後停止。既存資産を活用しつつコスト最小化。 |
| 3 | EC2常時稼働 ＋ SSM遠隔実行 | 既存EC2 | ◎ 最小 | ✕ 高い | 〇 分離 | 既存EC2を残しMWAAから `SsmOperator` で呼び出す。構成変更が最も少ないがサーバー代が高い。 |
| 4 | 任意S3配置 ＋ Worker実行 | MWAA Worker | 〇 低 | ◎ ゼロ | ◎ 完全分離 | S3からWorkerの `/tmp` にDLしてBash実行。軽量スクリプト向け。 |

---

## 4. アプリケーションチーム向け標準テンプレート

アプリケーションチームは、以下のテンプレートをコピー・編集してDAGを作成します。

### ① ECS Fargate 実行テンプレート（`dags/apps/templates/ecs_task_sample_dag.py`）

```python
"""ECS Fargate タスクを実行するアプリケーションDAGの標準テンプレート。"""
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

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
```

### ② EC2 / SSM スクリプト実行テンプレート（`dags/apps/templates/ssm_script_sample_dag.py`）

```python
"""EC2 上のスクリプトを AWS Systems Manager (SSM) で実行するDAGテンプレート。"""
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
```