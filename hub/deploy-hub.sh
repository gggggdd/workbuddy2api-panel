#!/bin/bash
# 安全更新 hub 二进制：备份 → 原子替换 → 单步重建（不 down）→ 健康检查，
# 失败自动回滚。可用性优先：任何一步失败都不让 7863 处于无监听状态。
# 用法：sudo bash deploy-hub.sh <新二进制路径>
set -uo pipefail
NEW="${1:?用法: deploy-hub.sh <新二进制路径>}"
DIR=/home/ubuntu/hub-gateway
cd "$DIR" || exit 1
[ -f "$NEW" ] && [ -s "$NEW" ] || { echo "新二进制不存在或为空: $NEW"; exit 1; }
STAMP=$(date +%m%d-%H%M%S)
cp -f hub "hub.bak-$STAMP" || { echo "备份失败，终止"; exit 1; }
cp -f "$NEW" hub.new && mv -f hub.new hub || { echo "替换失败，服务未受影响"; exit 1; }
docker compose up -d --force-recreate >/dev/null 2>&1 || true
for i in $(seq 1 10); do
  sleep 1
  if curl -sf -m 3 http://127.0.0.1:7863/healthz >/dev/null 2>&1; then
    echo "OK: hub 已更新且健康（备份 hub.bak-$STAMP）"
    exit 0
  fi
done
echo "健康检查失败 → 自动回滚旧版"
cp -f "hub.bak-$STAMP" hub
docker compose up -d --force-recreate >/dev/null 2>&1 || true
sleep 3
if curl -sf -m 3 http://127.0.0.1:7863/healthz >/dev/null 2>&1; then
  echo "已回滚到旧版并恢复服务"
else
  echo "回滚后仍不健康，请人工介入！"
fi
exit 1
