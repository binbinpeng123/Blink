#!/usr/bin/env bash
# 冒烟检查：构建并启动 blink，验证 /healthz、/readyz、/version 三个非 S3 端点，
# 以及一个 S3 路由返回规范的 501 NotImplemented 错误体。
#
# 该脚本刻意使用固定端口与临时数据目录，且用测试凭据 —— 只用于本地验证，
# 不得用于任何真实部署。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ADDR="${BLINK_SMOKE_ADDR:-127.0.0.1:19000}"
BASE="http://${ADDR}"
BIN="bin/blink"

TMP_DIR="$(mktemp -d)"
PID=""

cleanup() {
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  echo "--- 服务日志 ---" >&2
  cat "$TMP_DIR/blink.log" >&2 2>/dev/null || true
  exit 1
}

echo "==> 构建"
make build >/dev/null

echo "==> 启动服务 @ ${BASE}"
BLINK_ACCESS_KEY="AKIATESTTEST1234" \
BLINK_SECRET_KEY="test-secret-key-not-a-real-credential" \
  "$BIN" --addr "$ADDR" --data-dir "$TMP_DIR/data" --log-level debug \
  >"$TMP_DIR/blink.log" 2>&1 &
PID=$!

# 最多等 5 秒，直到 /healthz 可访问。
for _ in $(seq 1 50); do
  if curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

echo "==> GET /healthz"
BODY="$(curl -fsS "${BASE}/healthz")" || fail "/healthz 不可访问"
[[ "$BODY" == *'"status": "ok"'* ]] || fail "/healthz 响应体异常: $BODY"
echo "$BODY"

echo "==> GET /readyz"
BODY="$(curl -fsS "${BASE}/readyz")" || fail "/readyz 不可访问"
[[ "$BODY" == *'"ready": true'* ]] || fail "/readyz 响应体异常: $BODY"
echo "$BODY"

echo "==> GET /version"
BODY="$(curl -fsS "${BASE}/version")" || fail "/version 不可访问"
[[ "$BODY" == *'"version"'* ]] || fail "/version 响应体异常: $BODY"
echo "$BODY"

echo "==> 请求 ID 头"
REQ_ID="$(curl -fsS -D - -o /dev/null "${BASE}/healthz" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-amz-request-id"{print $2}')"
[[ -n "$REQ_ID" ]] || fail "响应缺少 x-amz-request-id"
echo "x-amz-request-id: $REQ_ID"

echo "==> 未签名的 S3 请求必须被拒绝"
# 切片 2 起 S3 路由统一校验 SigV4，未签名请求回 403 AccessDenied。
# （已实现/未实现的路由要到签名通过之后才区分，见 aws-cli-smoke.sh。）
CODE="$(curl -s -o "$TMP_DIR/s3.xml" -w '%{http_code}' "${BASE}/some-bucket/some-key")"
[[ "$CODE" == "403" ]] || fail "S3 路由状态码 = $CODE, 期望 403"
grep -q '<Code>AccessDenied</Code>' "$TMP_DIR/s3.xml" || fail "错误体缺少 AccessDenied: $(cat "$TMP_DIR/s3.xml")"
grep -q '<RequestId>' "$TMP_DIR/s3.xml" || fail "错误体缺少 RequestId"
echo "403 AccessDenied OK"

echo "==> 缺少凭据时应拒绝启动"
if BLINK_ACCESS_KEY= BLINK_SECRET_KEY= "$BIN" --addr "$ADDR" >"$TMP_DIR/nocred.log" 2>&1; then
  fail "缺少凭据时进程不应启动"
fi
grep -q 'Access Key' "$TMP_DIR/nocred.log" || fail "拒绝启动时未说明缺少凭据"
echo "无凭据拒绝启动 OK"

echo
echo "全部冒烟检查通过 ✅"