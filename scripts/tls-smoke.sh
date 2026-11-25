#!/usr/bin/env bash
# TLS 信任链冒烟：现场生成本地 CA 与服务器证书，启动 blink 的 HTTPS 模式，
# 用真实 aws-cli 通过 AWS_CA_BUNDLE 校验信任链完成 S3 读写。
#
# 与 tls_test.go 的分工：
#   tls_test.go —— Go 客户端，覆盖拒绝矩阵（未知 CA / 主机名 / TLS 1.1 / 明文）；
#   本脚本      —— 生态客户端，证明 aws-cli 不需要 --no-verify-ssl 也能用。
#
# 固定端口、临时目录与测试凭据 —— 只用于本地验证，不得指向任何真实服务。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ADDR="${BLINK_SMOKE_TLS_ADDR:-127.0.0.1:19443}"
BASE="https://${ADDR}"
BIN="bin/blink"
BUCKET="blink-tls-$$"
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
  echo "--- 服务日志（尾 100 行）---" >&2
  tail -n 100 "$TMP_DIR/blink.log" >&2 2>/dev/null || true
  exit 1
}

for dep in aws curl cmp dd openssl; do
  command -v "$dep" >/dev/null 2>&1 || {
    echo "缺少依赖: $dep（本脚本需要 aws-cli v2 与 openssl）" >&2
    exit 1
  }
done

# ---- 现场生成本地 CA 与 leaf --------------------------------------------
echo "==> 生成本地 CA 与服务器证书"
cat >"$TMP_DIR/leaf.ext" <<'EOF'
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost,IP:127.0.0.1
EOF

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$TMP_DIR/ca.key" -out "$TMP_DIR/ca.pem" -days 2 \
  -subj "/CN=Blink TLS Smoke CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null \
  || fail "生成 CA 失败"
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$TMP_DIR/server.key" -out "$TMP_DIR/server.csr" \
  -subj "/CN=localhost" 2>/dev/null || fail "生成服务器私钥失败"
openssl x509 -req -in "$TMP_DIR/server.csr" -CA "$TMP_DIR/ca.pem" \
  -CAkey "$TMP_DIR/ca.key" -CAcreateserial -days 2 \
  -extfile "$TMP_DIR/leaf.ext" -out "$TMP_DIR/server.pem" 2>/dev/null \
  || fail "签发服务器证书失败"

# 仅测试用凭据，全部经环境变量注入；不读 ~/.aws。
export AWS_ACCESS_KEY_ID="AKIATESTTEST1234"
export AWS_SECRET_ACCESS_KEY="test-secret-key-not-a-real-credential"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_EC2_METADATA_DISABLED="true"
export AWS_PAGER=""
export AWS_RETRY_MODE="standard"
export AWS_MAX_ATTEMPTS="1"

aws() { command aws --endpoint-url "$BASE" --no-cli-pager "$@"; }

echo "==> 构建"
make build >/dev/null

# 本脚本只写 1 KiB；开发机数据卷常已高于默认 0.90 水位，放宽以免把环境
# 误报成 TLS 失败。生产的默认水位不受影响。
echo "==> 启动服务 @ ${BASE}"
BLINK_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
BLINK_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" \
  "$BIN" --addr "$ADDR" --data-dir "$TMP_DIR/data" --log-level debug \
  --disk-high-watermark 0.99 --disk-low-watermark 0.98 \
  --tls-cert "$TMP_DIR/server.pem" --tls-key "$TMP_DIR/server.key" \
  >"$TMP_DIR/blink.log" 2>&1 &
PID=$!

for _ in $(seq 1 50); do
  if curl -fsS --cacert "$TMP_DIR/ca.pem" "${BASE}/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

# ---- 1. 探针 -------------------------------------------------------------
echo "==> [1/6] /healthz 经信任链可达"
curl -fsS --cacert "$TMP_DIR/ca.pem" "${BASE}/healthz" | grep -q '"status": "ok"' \
  || fail "/healthz 未就绪"

# ---- 2. aws-cli 信任链 ---------------------------------------------------
echo "==> [2/6] aws-cli 经 AWS_CA_BUNDLE 访问"
export AWS_CA_BUNDLE="$TMP_DIR/ca.pem"
aws s3api create-bucket --bucket "$BUCKET" --output text >/dev/null || fail "create-bucket 失败"
aws s3api head-bucket --bucket "$BUCKET" || fail "head-bucket 失败"

# ---- 3. 小对象往返 -------------------------------------------------------
echo "==> [3/6] 对象 PUT/HEAD/GET 往返"
dd if=/dev/urandom of="$TMP_DIR/small.bin" bs=1024 count=1 2>/dev/null
aws s3api put-object --bucket "$BUCKET" --key small.bin \
  --body "$TMP_DIR/small.bin" >/dev/null || fail "put-object 失败"
SIZE="$(aws s3api head-object --bucket "$BUCKET" --key small.bin \
  --query ContentLength --output text)" || fail "head-object 失败"
[[ "$SIZE" == "1024" ]] || fail "head-object ContentLength=$SIZE，期望 1024"
aws s3api get-object --bucket "$BUCKET" --key small.bin \
  "$TMP_DIR/small.out" >/dev/null || fail "get-object 失败"
cmp -s "$TMP_DIR/small.bin" "$TMP_DIR/small.out" || fail "对象内容不一致"

# ---- 4. Presigned URL ----------------------------------------------------
echo "==> [4/6] Presigned URL 下载"
URL="$(aws s3 presign "s3://$BUCKET/small.bin" --expires-in 120)" \
  || fail "presign 失败"
[[ "$URL" == https://* ]] || fail "presign 未返回 https URL: $URL"
curl -fsS --cacert "$TMP_DIR/ca.pem" "$URL" -o "$TMP_DIR/small.presigned" \
  || fail "Presigned URL 下载失败"
cmp -s "$TMP_DIR/small.bin" "$TMP_DIR/small.presigned" || fail "Presigned 内容不一致"

# ---- 5. 负例：不信任 CA ------------------------------------------------
echo "==> [5/6] 未信任 CA 的客户端必须失败"
if env -u AWS_CA_BUNDLE aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
  fail "aws-cli 未配置 CA 时竟然成功，信任链未生效"
fi
if curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then
  fail "curl 默认信任库访问成功，证书未被真正校验"
fi

# ---- 6. 负例：TLS 版本下限 ----------------------------------------------
echo "==> [6/6] TLS 1.1 客户端必须握手失败"
if curl -fsS --tlsv1.1 --tls-max 1.1 --cacert "$TMP_DIR/ca.pem" \
  "${BASE}/healthz" >/dev/null 2>&1; then
  fail "TLS 1.1 握手成功，MinVersion 配置未生效"
fi

echo "==> TLS 信任链冒烟通过（aws-cli + curl，未使用 --no-verify-ssl）"