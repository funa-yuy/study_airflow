#!/bin/bash
# =============================================================
# [モック] init.sh
# 本番では環境変数（AWS認証情報、ECS設定など）を export する。
# テスト環境ではモック値を設定する。
#
# 使い方（ecs_run.sh内から呼ばれる想定）:
#   source "$(dirname "$0")/init.sh"
# =============================================================

echo "[init.sh] 環境変数の初期化（モック）"

# 本番では以下の変数が実際の値になる
export ECS_CLUSTER="${ECS_CLUSTER:-mock-cluster}"
export ECS_TASK_DEFINITION="${ECS_TASK_DEFINITION:-mock-task:1}"
export AWS_REGION="${AWS_REGION:-ap-northeast-1}"
export DEPLOY_ENV="${DEPLOY_ENV:-local}"

echo "[init.sh] ECS_CLUSTER       = ${ECS_CLUSTER}"
echo "[init.sh] ECS_TASK_DEFINITION = ${ECS_TASK_DEFINITION}"
echo "[init.sh] AWS_REGION        = ${AWS_REGION}"
echo "[init.sh] DEPLOY_ENV        = ${DEPLOY_ENV}"
echo "[init.sh] 環境変数の初期化が完了しました"
