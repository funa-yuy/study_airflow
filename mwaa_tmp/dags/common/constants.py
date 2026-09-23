"""
全DAG共通の接続先定数およびデフォルト値。
各チームのDAGやYAML設定で必要に応じて上書き可能です。
"""

# Redshift 接続デフォルト値
DEFAULT_REDSHIFT_CONN_ID = "aws_default"
DEFAULT_REDSHIFT_DATABASE = "analytics"
DEFAULT_REDSHIFT_WORKGROUP = "prod-workgroup"  # Redshift Serverless のワークグループ名

# アラート通知接続
DEFAULT_SLACK_CONN_ID = "slack_alert_default"
