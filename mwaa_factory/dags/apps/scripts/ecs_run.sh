#!/bin/bash
# =============================================================
# [モック] ecs_run.sh
# 本番では `aws ecs run-task` を呼び出すスクリプト。
# テスト環境では ECS タスクを起動せず、受け取った引数を表示するだけ。
#
# 使い方:
#   bash ecs_run.sh <config_yml_path>
#
# 引数:
#   $1 : ジョブ設定YMLファイルのパス（job_config.yml）
# =============================================================
set -euo pipefail

CONFIG_YML="${1:-}"

echo "=================================================="
echo "[ecs_run.sh] ECS タスク起動スクリプト（モック）"
echo "=================================================="

if [ -z "${CONFIG_YML}" ]; then
  echo "[ERROR] 設定YMLファイルが指定されていません。"
  echo "使い方: bash ecs_run.sh <config_yml_path>"
  exit 1
fi

if [ ! -f "${CONFIG_YML}" ]; then
  echo "[ERROR] 指定された設定ファイルが見つかりません: ${CONFIG_YML}"
  exit 1
fi

echo "[INFO] 設定ファイル: ${CONFIG_YML}"
echo "[INFO] --- 設定内容 ---"
cat "${CONFIG_YML}"
echo ""
echo "[INFO] -------------------"

# 本番では以下のような処理に置き換える:
# source "$(dirname "$0")/init.sh"
# aws ecs run-task \
#   --cluster "${ECS_CLUSTER}" \
#   --task-definition "${ECS_TASK_DEFINITION}" \
#   --launch-type FARGATE \
#   ...

echo "[MOCK] ECS タスクを起動しました（モック実行）"
echo "[MOCK] 実際の ECS タスクは起動されていません"
exit 0
