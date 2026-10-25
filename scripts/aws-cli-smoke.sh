#!/usr/bin/env bash
# 端到端互操作验收：用真实 aws-cli 跑一遍 02 文档承诺的 S3 能力边界。
#
# 与 health-smoke.sh 的分工：
#   health-smoke.sh —— 不签名，只验证进程能起来、探针可达、未签名请求被拒；
#   本脚本        —— 全流程签名，验证"任意 S3 客户端可用"这个核心卖点。
#
# 12 步对应开发计划切片 7 的验收标准（multipart 全流程）：
#   1 healthz / 2 bucket 生命周期 / 3 小对象往返 / 4 大对象 multipart 往返
#   5 Range / 6 HEAD / 7 显式 CRC64NVME 与 SHA256 / 8 checksum-mode
#   9 手工 multipart 全流程（含桶级会话列表）/ 10 sync / 11 presign
#   12 不支持项 501（含 partNumber 分片下载）/ 清理
#
# 脚本自带构建、启动与清理，使用固定端口和测试凭据 —— 只用于本地验证，
# 不得指向任何真实服务。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ADDR="${BLINK_SMOKE_ADDR:-127.0.0.1:19002}"
BASE="http://${ADDR}"
BIN="bin/blink"
BUCKET="blink-smoke-$$"
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

for dep in aws curl cmp dd diff; do
  command -v "$dep" >/dev/null 2>&1 || {
    echo "缺少依赖: $dep（本脚本需要 aws-cli v2）" >&2
    exit 1
  }
done

# aws-cli 的全部凭据都来自环境变量：不读 ~/.aws，避免本机已有配置
# 意外指向真实账号。AWS_EC2_METADATA_DISABLED 防止它在缺凭据时去撞 IMDS
# （那会拖慢每条命令几十秒）。
export AWS_ACCESS_KEY_ID="AKIATESTTEST1234"
export AWS_SECRET_ACCESS_KEY="test-secret-key-not-a-real-credential"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_EC2_METADATA_DISABLED="true"
export AWS_PAGER=""
export AWS_RETRY_MODE="standard"
# 冒烟只关心首次结果：失败立即暴露，而不是被 SDK 重试掩盖。
export AWS_MAX_ATTEMPTS="1"

aws() { command aws --endpoint-url "$BASE" --no-cli-pager "$@"; }

echo "==> 构建"
make build >/dev/null

echo "==> 启动服务 @ ${BASE}"
BLINK_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
BLINK_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" \
  "$BIN" --addr "$ADDR" --data-dir "$TMP_DIR/data" --log-level debug \
  >"$TMP_DIR/blink.log" 2>&1 &
PID=$!

for _ in $(seq 1 50); do
  if curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.1
done

# ---- 1. healthz ---------------------------------------------------------
echo "==> [1/12] healthz"
curl -fsS "${BASE}/healthz" | grep -q '"status": "ok"' || fail "/healthz 未就绪"

# ---- 2. bucket 生命周期 --------------------------------------------------
echo "==> [2/12] bucket 生命周期"
aws s3api create-bucket --bucket "$BUCKET" >/dev/null || fail "create-bucket 失败"
aws s3api head-bucket --bucket "$BUCKET" || fail "head-bucket 失败"
aws s3api list-buckets --query 'Buckets[].Name' --output text \
  | tr '\t' '\n' | grep -qx "$BUCKET" || fail "list-buckets 中看不到 $BUCKET"

# ---- 3. 小对象往返 -------------------------------------------------------
echo "==> [3/12] 小对象往返"
dd if=/dev/urandom of="$TMP_DIR/small.bin" bs=1024 count=100 2>/dev/null
aws s3 cp "$TMP_DIR/small.bin" "s3://$BUCKET/small.bin" --only-show-errors \
  || fail "小对象上传失败"
aws s3 cp "s3://$BUCKET/small.bin" "$TMP_DIR/small.down" --only-show-errors \
  || fail "小对象下载失败"
cmp "$TMP_DIR/small.bin" "$TMP_DIR/small.down" || fail "小对象内容不一致"

# ---- 4. 大对象（multipart）往返 -----------------------------------------
# 12 MiB > aws-cli 默认 multipart 阈值 8 MiB，因此这条命令必然走
# CreateMultipartUpload / UploadPart / CompleteMultipartUpload 全链路。
# 默认校验和是 CRC64NVME，客户端会在下载时用服务端返回的完整对象校验和复算，
# 这一步同时验证了"Complete 只串接块表、不重写数据"的正确性。
echo "==> [4/12] 大对象 multipart 往返"
dd if=/dev/urandom of="$TMP_DIR/big.bin" bs=1048576 count=12 2>/dev/null
aws s3 cp "$TMP_DIR/big.bin" "s3://$BUCKET/big.bin" --only-show-errors \
  || fail "大对象 multipart 上传失败"
ETAG="$(aws s3api head-object --bucket "$BUCKET" --key big.bin --query ETag --output text)"
case "$ETAG" in
  *-*) ;;
  *) fail "multipart ETag 应带 -<N> 后缀，实际 $ETAG" ;;
esac
aws s3 cp "s3://$BUCKET/big.bin" "$TMP_DIR/big.down" --only-show-errors \
  || fail "大对象下载失败"
cmp "$TMP_DIR/big.bin" "$TMP_DIR/big.down" || fail "大对象内容不一致"

# ---- 5. Range -----------------------------------------------------------
echo "==> [5/12] Range 请求"
aws s3api get-object --bucket "$BUCKET" --key big.bin \
  --range bytes=0-1023 "$TMP_DIR/range.down" >/dev/null || fail "Range 请求失败"
head -c 1024 "$TMP_DIR/big.bin" >"$TMP_DIR/range.expected"
cmp "$TMP_DIR/range.expected" "$TMP_DIR/range.down" || fail "Range 内容不一致"

# ---- 6. HEAD ------------------------------------------------------------
echo "==> [6/12] HEAD 元数据"
printf 'edge metering payload\n' >"$TMP_DIR/meta.txt"
aws s3api put-object --bucket "$BUCKET" --key meta/owner.txt \
  --body "$TMP_DIR/meta.txt" --content-type "text/plain; charset=utf-8" \
  --metadata owner=edge-01,site=bj >/dev/null || fail "带元数据的 PUT 失败"
# 逐个字段查询：Content-Type 本身含空格，用 --output text 拼多字段
# 再按空白切分会把 "text/plain; charset=utf-8" 拦腰截断。
HEAD_LEN="$(aws s3api head-object --bucket "$BUCKET" --key meta/owner.txt \
  --query ContentLength --output text)"
HEAD_CT="$(aws s3api head-object --bucket "$BUCKET" --key meta/owner.txt \
  --query ContentType --output text)"
HEAD_OWNER="$(aws s3api head-object --bucket "$BUCKET" --key meta/owner.txt \
  --query Metadata.owner --output text)"
[[ "$HEAD_LEN" == "22" ]] || fail "HEAD Content-Length = $HEAD_LEN，期望 22"
[[ "$HEAD_CT" == "text/plain; charset=utf-8" ]] || fail "HEAD Content-Type = $HEAD_CT"
[[ "$HEAD_OWNER" == "edge-01" ]] || fail "HEAD 用户元数据 owner = $HEAD_OWNER"

# ---- 7. 显式校验和 -------------------------------------------------------
echo "==> [7/12] 显式 CRC64NVME / SHA256"
read -r CRC_VAL CRC_TYPE <<EOF
$(aws s3api put-object --bucket "$BUCKET" --key crc.bin \
  --body "$TMP_DIR/small.bin" --checksum-algorithm CRC64NVME \
  --query '[ChecksumCRC64NVME, ChecksumType]' --output text)
EOF
[[ -n "$CRC_VAL" && "$CRC_VAL" != "None" ]] || fail "未回传 ChecksumCRC64NVME"
[[ "$CRC_TYPE" == "FULL_OBJECT" ]] || fail "CRC64NVME 类型 = $CRC_TYPE，期望 FULL_OBJECT"

WANT_SHA="$(openssl dgst -sha256 -binary "$TMP_DIR/small.bin" | base64)"
read -r SHA_VAL SHA_TYPE <<EOF
$(aws s3api put-object --bucket "$BUCKET" --key sha.bin \
  --body "$TMP_DIR/small.bin" --checksum-algorithm SHA256 \
  --query '[ChecksumSHA256, ChecksumType]' --output text)
EOF
[[ "$SHA_VAL" == "$WANT_SHA" ]] || fail "SHA256 = $SHA_VAL，期望 $WANT_SHA"
[[ "$SHA_TYPE" == "FULL_OBJECT" ]] || fail "单次 PUT 的 SHA256 类型 = $SHA_TYPE，期望 FULL_OBJECT"

# ---- 8. checksum-mode ---------------------------------------------------
echo "==> [8/12] GET checksum-mode"
read -r GET_CRC GET_TYPE <<EOF
$(aws s3api get-object --bucket "$BUCKET" --key crc.bin \
  --checksum-mode ENABLED "$TMP_DIR/crc.down" \
  --query '[ChecksumCRC64NVME, ChecksumType]' --output text)
EOF
[[ "$GET_CRC" == "$CRC_VAL" ]] || fail "GET 校验和 = $GET_CRC，与 PUT 时 $CRC_VAL 不一致"
[[ "$GET_TYPE" == "FULL_OBJECT" ]] || fail "GET checksum-type = $GET_TYPE，期望 FULL_OBJECT"
cmp "$TMP_DIR/small.bin" "$TMP_DIR/crc.down" || fail "checksum-mode 下载内容不一致"

# ---- 9. 手工 multipart 全流程 -------------------------------------------
echo "==> [9/12] 手工 multipart（create / upload-part / list-parts / list-uploads / abort / complete）"
dd if=/dev/urandom of="$TMP_DIR/p1.bin" bs=1048576 count=5 2>/dev/null
dd if=/dev/urandom of="$TMP_DIR/p2.bin" bs=1024 count=1 2>/dev/null
cat "$TMP_DIR/p1.bin" "$TMP_DIR/p2.bin" >"$TMP_DIR/manual.expected"

UPLOAD_ID="$(aws s3api create-multipart-upload --bucket "$BUCKET" --key manual.bin \
  --query UploadId --output text)" || fail "create-multipart-upload 失败"
[[ -n "$UPLOAD_ID" && "$UPLOAD_ID" != "None" ]] || fail "UploadId 为空"

E1="$(aws s3api upload-part --bucket "$BUCKET" --key manual.bin --upload-id "$UPLOAD_ID" \
  --part-number 1 --body "$TMP_DIR/p1.bin" --query ETag --output text)"
E2="$(aws s3api upload-part --bucket "$BUCKET" --key manual.bin --upload-id "$UPLOAD_ID" \
  --part-number 2 --body "$TMP_DIR/p2.bin" --query ETag --output text)"
E1="${E1//\"/}"
E2="${E2//\"/}"
[[ -n "$E1" && -n "$E2" ]] || fail "upload-part 未返回 ETag"

PARTS="$(aws s3api list-parts --bucket "$BUCKET" --key manual.bin --upload-id "$UPLOAD_ID" \
  --query 'Parts[].PartNumber' --output text)"
[[ "$PARTS" == "1	2" || "$PARTS" == "1 2" ]] || fail "list-parts 返回 $PARTS，期望 1 2"

# 单独开一个会话再 Abort：验证清理语义，且不影响上面这个会话。
ABORT_ID="$(aws s3api create-multipart-upload --bucket "$BUCKET" --key aborted.bin \
  --query UploadId --output text)"
aws s3api upload-part --bucket "$BUCKET" --key aborted.bin --upload-id "$ABORT_ID" \
  --part-number 1 --body "$TMP_DIR/p2.bin" >/dev/null

# 桶级会话列表：此刻两个会话都在进行中。这是六条 multipart 路由里
# 唯一需要跨 key 观察的，只有真实客户端调用一次才算验证过。
ACTIVE="$(aws s3api list-multipart-uploads --bucket "$BUCKET" \
  --query 'Uploads[].Key' --output text)" || fail "list-multipart-uploads 失败"
for k in manual.bin aborted.bin; do
  grep -qw -- "$k" <<<"$ACTIVE" || fail "list-multipart-uploads 缺少 $k：$ACTIVE"
done

aws s3api abort-multipart-upload --bucket "$BUCKET" --key aborted.bin \
  --upload-id "$ABORT_ID" || fail "abort-multipart-upload 失败"
if aws s3api list-parts --bucket "$BUCKET" --key aborted.bin \
  --upload-id "$ABORT_ID" >/dev/null 2>&1; then
  fail "Abort 之后 list-parts 仍应报 NoSuchUpload"
fi
ACTIVE="$(aws s3api list-multipart-uploads --bucket "$BUCKET" \
  --query 'Uploads[].Key' --output text)"
if grep -qw -- "aborted.bin" <<<"$ACTIVE"; then
  fail "Abort 之后 list-multipart-uploads 仍列出 aborted.bin：$ACTIVE"
fi

cat >"$TMP_DIR/complete.json" <<EOF
{"Parts":[{"PartNumber":1,"ETag":"$E1"},{"PartNumber":2,"ETag":"$E2"}]}
EOF
DONE_ETAG="$(aws s3api complete-multipart-upload --bucket "$BUCKET" --key manual.bin \
  --upload-id "$UPLOAD_ID" --multipart-upload "file://$TMP_DIR/complete.json" \
  --query ETag --output text)" || fail "complete-multipart-upload 失败"
case "$DONE_ETAG" in
  *-2*) ;;
  *) fail "Complete 的 ETag 应带 -2 后缀，实际 $DONE_ETAG" ;;
esac
aws s3api get-object --bucket "$BUCKET" --key manual.bin "$TMP_DIR/manual.down" >/dev/null \
  || fail "手工 multipart 对象下载失败"
cmp "$TMP_DIR/manual.expected" "$TMP_DIR/manual.down" \
  || fail "手工 multipart 对象内容与分片拼接不一致"

# ---- 10. sync ------------------------------------------------------------
echo "==> [10/12] sync"
mkdir -p "$TMP_DIR/sync-src/sub"
printf 'alpha\n' >"$TMP_DIR/sync-src/a.txt"
printf 'beta\n' >"$TMP_DIR/sync-src/sub/b.txt"
dd if=/dev/urandom of="$TMP_DIR/sync-src/sub/c.bin" bs=1024 count=200 2>/dev/null
aws s3 sync "$TMP_DIR/sync-src" "s3://$BUCKET/sync/" --only-show-errors \
  || fail "sync 上传失败"
COUNT="$(aws s3 ls "s3://$BUCKET/sync/" --recursive | wc -l | tr -d ' ')"
[[ "$COUNT" == "3" ]] || fail "sync 后对象数 = $COUNT，期望 3"
aws s3 sync "s3://$BUCKET/sync/" "$TMP_DIR/sync-dst" --only-show-errors \
  || fail "sync 下载失败"
diff -r "$TMP_DIR/sync-src" "$TMP_DIR/sync-dst" >/dev/null \
  || fail "sync 往返目录内容不一致"

# ---- 11. presign ---------------------------------------------------------
echo "==> [11/12] presign"
URL="$(aws s3 presign "s3://$BUCKET/small.bin" --expires-in 300)" \
  || fail "presign 失败"
curl -fsS "$URL" -o "$TMP_DIR/presign.down" || fail "presigned URL 请求失败"
cmp "$TMP_DIR/small.bin" "$TMP_DIR/presign.down" || fail "presigned 下载内容不一致"

# ---- 12. 不支持项必须明确 501 -------------------------------------------
echo "==> [12/12] 不支持项返回 501 NotImplemented"
for op in get-bucket-acl get-bucket-versioning; do
  OUT="$(aws s3api "$op" --bucket "$BUCKET" 2>&1)" && fail "$op 应失败但成功了"
  case "$OUT" in
    *NotImplemented*) ;;
    *) fail "$op 的错误不是 NotImplemented: $OUT" ;;
  esac
done
# 按 partNumber 下载对象分片（GetObject 子资源）同样必须明确 501：
# 静默返回整个对象会让客户端把它当成第 1 片，拼出重复数据。
if OUT="$(aws s3api get-object --bucket "$BUCKET" --key small.bin \
  --part-number 1 "$TMP_DIR/part.down" 2>&1)"; then
  fail "get-object --part-number 应返回 501"
fi
case "$OUT" in
  *NotImplemented*) ;;
  *) fail "get-object --part-number 的错误不是 NotImplemented: $OUT" ;;
esac

# ---- 清理 ---------------------------------------------------------------
echo "==> 清理"
aws s3 rm "s3://$BUCKET" --recursive --only-show-errors || fail "递归删除对象失败"
aws s3api delete-bucket --bucket "$BUCKET" || fail "delete-bucket 失败"
if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
  fail "删除后 head-bucket 仍然成功"
fi

echo
echo "aws-cli 互操作冒烟全部通过 ✅"
