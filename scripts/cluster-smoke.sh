#!/usr/bin/env bash
# 集群冒烟：在本机起 N 个 blink 进程，验证跨节点读写、LIST 与桶控制面。
#
# 覆盖项：
#   - 桶只在 1 个节点创建，其余节点立即可见（重复创建回 409）；
#   - 任一节点写入、其余节点读出，内容逐字节一致；
#   - 跨块 Range 读取；
#   - 覆盖写后所有节点收敛到新内容；
#   - 删除后所有节点立即 404，且 LIST 不再返回该 key；
#   - 任意节点的 LIST 结果与对象集合一致（含 prefix/delimiter）；
#   - CORS 配置写入 1 个节点后其余节点可读；
#   - 非空桶删除被拒；清空后删桶对所有节点生效；
#   - 杀掉 1 个节点后仍可读写（W=2 / R=2 仍可满足）；
#   - 杀掉 2 个节点后读写回 503，而不是假装对象不存在。
#
# 该脚本只用固定端口、临时目录与测试凭据，仅用于本地验证，
# 不得指向任何真实部署。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

N="${1:-3}"
if [[ "$N" != "3" ]]; then
  echo "用法: $0 [3]" >&2
  echo "当前切片只验证 N=3 的副本读写；EC/6 节点形态在切片 7 引入。" >&2
  exit 2
fi

BIN="bin/blink"
BUCKET="cluster-smoke-$(date +%s)"
S3_PORT_BASE=19101
INTERNAL_PORT_BASE=19201
PEER_TIMEOUT="500ms"

TMP_DIR="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for i in $(seq 0 $((N - 1))); do
    echo "--- 节点 $((i + 1)) 日志（末 40 行）---" >&2
    tail -n 40 "$TMP_DIR/node${i}.log" >&2 2>/dev/null || true
  done
  exit 1
}

s3_port() { echo $((S3_PORT_BASE + $1)); }
internal_port() { echo $((INTERNAL_PORT_BASE + $1)); }

# 测试凭据：写死在这里是刻意的，脚本只跑在 127.0.0.1。
export AWS_ACCESS_KEY_ID="AKIATESTTEST1234"
export AWS_SECRET_ACCESS_KEY="test-secret-key-not-a-real-credential"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_EC2_METADATA_DISABLED="true"
export AWS_PAGER=""
export AWS_RETRY_MODE="standard"
export AWS_MAX_ATTEMPTS="1"

# aws_at <节点序号> <s3api 子命令...>
aws_at() {
  local idx="$1"
  shift
  command aws --endpoint-url "http://127.0.0.1:$(s3_port "$idx")" --no-cli-pager "$@"
}

# 内部协议共享密钥：仅测试用，长度必须 ≥32 字节。
CLUSTER_SECRET="cluster-smoke-secret-not-a-real-credential"

# 成员表必须包含自身，且所有节点看到的是同一份。
PEERS=""
for i in $(seq 0 $((N - 1))); do
  [[ -n "$PEERS" ]] && PEERS+=","
  PEERS+="node$((i + 1))@127.0.0.1:$(internal_port "$i")"
done

echo "==> 构建"
make build >/dev/null

echo "==> 启动 ${N} 个节点"
for i in $(seq 0 $((N - 1))); do
  mkdir -p "$TMP_DIR/node${i}/data"
  BLINK_CLUSTER_MODE=cluster \
  BLINK_NODE_ID="node$((i + 1))" \
  BLINK_CLUSTER_PEERS="$PEERS" \
  BLINK_INTERNAL_ADDR="127.0.0.1:$(internal_port "$i")" \
  BLINK_CLUSTER_SECRET="$CLUSTER_SECRET" \
  BLINK_REPLICA_COUNT=3 \
  BLINK_WRITE_QUORUM=2 \
  BLINK_READ_QUORUM=2 \
    "$BIN" \
      --addr "127.0.0.1:$(s3_port "$i")" \
      --data-dir "$TMP_DIR/node${i}/data" \
      --access-key "$AWS_ACCESS_KEY_ID" \
      --secret-key "$AWS_SECRET_ACCESS_KEY" \
      --log-level debug \
      --verify-read \
      --peer-timeout "$PEER_TIMEOUT" \
      >"$TMP_DIR/node${i}.log" 2>&1 &
  PIDS+=("$!")
done

echo "==> 等待节点就绪"
for i in $(seq 0 $((N - 1))); do
  ready=""
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:$(s3_port "$i")/healthz" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.1
  done
  [[ -n "$ready" ]] || fail "节点 $((i + 1)) 未在 10 秒内就绪"
done

echo "==> 只在节点 1 建桶，验证广播到其余节点"
aws_at 0 s3api create-bucket --bucket "$BUCKET" >/dev/null \
  || fail "节点 1 建桶失败"
for i in $(seq 0 $((N - 1))); do
  aws_at "$i" s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 \
    || fail "节点 $((i + 1)) 看不到桶 ${BUCKET}（建桶未广播）"
  aws_at "$i" s3api list-buckets --query 'Buckets[].Name' --output text \
    | tr '\t' '\n' | grep -qx "$BUCKET" \
    || fail "节点 $((i + 1)) 的 list-buckets 缺少 ${BUCKET}"
done
if OUT="$(aws_at 2 s3api create-bucket --bucket "$BUCKET" 2>&1)"; then
  fail "在节点 3 重复建桶应失败，实际成功"
fi
grep -q "BucketAlreadyOwnedByYou" <<<"$OUT" \
  || fail "重复建桶错误不是 BucketAlreadyOwnedByYou: ${OUT}"

echo "==> 生成测试对象"
PAYLOAD="$TMP_DIR/payload.bin"
dd if=/dev/urandom of="$PAYLOAD" bs=1024 count=8192 2>/dev/null # 8 MiB，跨多个块
WANT_ETAG="$(md5 -q "$PAYLOAD")"
WANT_SIZE="$(wc -c <"$PAYLOAD" | tr -d ' ')"

echo "==> 节点 1 写入，节点 2/3 读取"
aws_at 0 s3api put-object --bucket "$BUCKET" --key big.bin --body "$PAYLOAD" >/dev/null \
  || fail "节点 1 写入失败"

for i in 1 2; do
  aws_at "$i" s3api get-object --bucket "$BUCKET" --key big.bin "$TMP_DIR/read${i}.bin" >/dev/null \
    || fail "节点 $((i + 1)) 读取失败"
  cmp -s "$PAYLOAD" "$TMP_DIR/read${i}.bin" \
    || fail "节点 $((i + 1)) 读到的内容与写入不一致"
done

echo "==> 校验 HEAD 元数据"
HEAD_ETAG="$(aws_at 2 s3api head-object --bucket "$BUCKET" --key big.bin --query ETag --output text \
  | tr -d '"')"
[[ "$HEAD_ETAG" == "$WANT_ETAG" ]] || fail "HEAD ETag=${HEAD_ETAG}，期望 ${WANT_ETAG}"
HEAD_SIZE="$(aws_at 2 s3api head-object --bucket "$BUCKET" --key big.bin --query ContentLength --output text)"
[[ "$HEAD_SIZE" == "$WANT_SIZE" ]] || fail "HEAD ContentLength=${HEAD_SIZE}，期望 ${WANT_SIZE}"

echo "==> 跨块 Range 读取"
# 偏移 1000 起 9000 字节；不用 tail|head，pipefail 会把 head 提前关闭
# 管道产生的 SIGPIPE 当成失败。
dd if="$PAYLOAD" of="$TMP_DIR/range.want" bs=1000 skip=1 count=9 2>/dev/null
aws_at 1 s3api get-object --bucket "$BUCKET" --key big.bin \
  --range "bytes=1000-9999" "$TMP_DIR/range.got" >/dev/null \
  || fail "Range 读取失败"
cmp -s "$TMP_DIR/range.want" "$TMP_DIR/range.got" \
  || fail "Range bytes=1000-9999 内容不一致"

echo "==> 覆盖写后所有节点收敛"
echo -n "new-content-after-overwrite" >"$TMP_DIR/small.bin"
aws_at 0 s3api put-object --bucket "$BUCKET" --key small.bin --body "$TMP_DIR/small.bin" >/dev/null \
  || fail "首次写入 small.bin 失败"
aws_at 2 s3api put-object --bucket "$BUCKET" --key small.bin --body "$PAYLOAD" >/dev/null \
  || fail "覆盖写 small.bin 失败"
for i in $(seq 0 $((N - 1))); do
  aws_at "$i" s3api get-object --bucket "$BUCKET" --key small.bin "$TMP_DIR/small${i}.got" >/dev/null \
    || fail "节点 $((i + 1)) 读取覆盖后的对象失败"
  cmp -s "$PAYLOAD" "$TMP_DIR/small${i}.got" \
    || fail "节点 $((i + 1)) 未收敛到覆盖后的内容"
done

# 跨节点 LIST：每个节点看到的对象集合必须完全相同。
# 对象轮流经不同节点写入，副本散落在不同节点上，归并逻辑只要漏一份就会露馅。
echo "==> 跨节点 LIST"
for i in 0 1 2; do
  aws_at "$i" s3api put-object --bucket "$BUCKET" --key "dir/obj-$i.bin" \
    --body "$TMP_DIR/small.bin" >/dev/null || fail "节点 $((i + 1)) 写入 dir/obj-$i.bin 失败"
done

list_keys() {
  # $1 = 节点序号；过滤掉空结果时 aws-cli 输出的 None。
  aws_at "$1" s3api list-objects-v2 --bucket "$BUCKET" \
    --query 'Contents[].Key' --output text 2>/dev/null \
    | tr '\t' '\n' | grep -vx 'None' | sed '/^$/d' | sort
}
WANT_KEYS="$(printf '%s\n' big.bin dir/obj-0.bin dir/obj-1.bin dir/obj-2.bin small.bin | sort)"
for i in $(seq 0 $((N - 1))); do
  GOT_KEYS="$(list_keys "$i")"
  [[ "$GOT_KEYS" == "$WANT_KEYS" ]] \
    || fail "节点 $((i + 1)) 的 LIST 结果不符\n期望:\n${WANT_KEYS}\n实际:\n${GOT_KEYS}"
done

# delimiter 折叠必须在归并之后进行：三个 dir/ 对象只应产生一个 CommonPrefix。
PREFIXES="$(aws_at 1 s3api list-objects-v2 --bucket "$BUCKET" --delimiter / \
  --query 'CommonPrefixes[].Prefix' --output text | tr '\t' '\n' | grep -vx 'None' | sort)"
[[ "$PREFIXES" == "dir/" ]] || fail "delimiter 折叠结果 = [${PREFIXES}]，期望 [dir/]"

echo "==> CORS 广播"
CORS_JSON="$TMP_DIR/cors.json"
cat >"$CORS_JSON" <<'JSON'
{"CORSRules":[{"ID":"smoke","AllowedOrigins":["https://example.com"],
"AllowedMethods":["GET","PUT"],"AllowedHeaders":["*"]}]}
JSON
aws_at 0 s3api put-bucket-cors --bucket "$BUCKET" --cors-configuration "file://$CORS_JSON" >/dev/null \
  || fail "节点 1 写入 CORS 失败"
for i in $(seq 0 $((N - 1))); do
  GET_CORS="$(aws_at "$i" s3api get-bucket-cors --bucket "$BUCKET" \
    --query 'CORSRules[0].ID' --output text)"
  [[ "$GET_CORS" == "smoke" ]] || fail "节点 $((i + 1)) 的 CORS = ${GET_CORS}，期望 smoke"
done

echo "==> 删除后所有节点立即 404，且 LIST 消失"
aws_at 1 s3api delete-object --bucket "$BUCKET" --key small.bin >/dev/null \
  || fail "删除失败"
for i in $(seq 0 $((N - 1))); do
  if aws_at "$i" s3api head-object --bucket "$BUCKET" --key small.bin >/dev/null 2>&1; then
    fail "节点 $((i + 1)) 删除后仍能 HEAD 到对象"
  fi
  # 墓碑必须参与归并：某个节点还留着删除前的活副本时，不能把它列出来。
  if list_keys "$i" | grep -qx "small.bin"; then
    fail "节点 $((i + 1)) 的 LIST 仍返回已删除的 small.bin"
  fi
done

# 桶删除是整集群操作：空判定必须覆盖全部节点，删除也要落到全部节点。
echo "==> 桶删除：非空拒绝，清空后全集群生效"
META_BUCKET="${BUCKET}-meta"
aws_at 1 s3api create-bucket --bucket "$META_BUCKET" >/dev/null \
  || fail "建桶 ${META_BUCKET} 失败"
aws_at 2 s3api put-object --bucket "$META_BUCKET" --key keep.bin \
  --body "$TMP_DIR/small.bin" >/dev/null || fail "写入 ${META_BUCKET}/keep.bin 失败"

if OUT="$(aws_at 0 s3api delete-bucket --bucket "$META_BUCKET" 2>&1)"; then
  fail "非空桶删除应失败，实际成功"
fi
grep -q "BucketNotEmpty" <<<"$OUT" || fail "非空桶删除错误不是 BucketNotEmpty: ${OUT}"

aws_at 0 s3api delete-object --bucket "$META_BUCKET" --key keep.bin >/dev/null \
  || fail "删除 ${META_BUCKET}/keep.bin 失败"
aws_at 2 s3api delete-bucket --bucket "$META_BUCKET" >/dev/null \
  || fail "清空后删桶失败"
for i in $(seq 0 $((N - 1))); do
  if aws_at "$i" s3api head-bucket --bucket "$META_BUCKET" >/dev/null 2>&1; then
    fail "节点 $((i + 1)) 删除后仍能看到桶 ${META_BUCKET}"
  fi
  if aws_at "$i" s3api list-buckets --query 'Buckets[].Name' --output text \
    | tr '\t' '\n' | grep -qx "$META_BUCKET"; then
    fail "节点 $((i + 1)) 的 list-buckets 仍包含 ${META_BUCKET}"
  fi
done

echo "==> 杀掉 1 个节点后仍可读写"
kill -9 "${PIDS[2]}" 2>/dev/null || true
wait "${PIDS[2]}" 2>/dev/null || true
PIDS[2]=""

aws_at 0 s3api put-object --bucket "$BUCKET" --key degraded.bin --body "$TMP_DIR/small.bin" >/dev/null \
  || fail "1 个节点宕机后写入失败（W=2 应仍可满足）"
aws_at 1 s3api get-object --bucket "$BUCKET" --key degraded.bin "$TMP_DIR/degraded.got" >/dev/null \
  || fail "1 个节点宕机后读取失败（R=2 应仍可满足）"
cmp -s "$TMP_DIR/small.bin" "$TMP_DIR/degraded.got" \
  || fail "1 个节点宕机后读到的内容不一致"

# 删桶与对象数据面相反：任一节点不可达就拒绝，否则宕机节点可能带着
# 旧桶（与旧对象）回归，造出"桶删了却还能读到数据"的状态。
if OUT="$(aws_at 0 s3api delete-bucket --bucket "$BUCKET" 2>&1)"; then
  fail "节点宕机时删桶应失败，实际成功"
fi
grep -q "ServiceUnavailable\|503" <<<"$OUT" \
  || fail "节点宕机时删桶错误不是 503: ${OUT}"

echo "==> 杀掉第 2 个节点后应返回 503"
kill -9 "${PIDS[1]}" 2>/dev/null || true
wait "${PIDS[1]}" 2>/dev/null || true
PIDS[1]=""

if OUT="$(aws_at 0 s3api get-object --bucket "$BUCKET" --key big.bin "$TMP_DIR/should-not-exist" 2>&1)"; then
  fail "只剩 1 个节点时读取应失败，实际成功"
fi
grep -q "ServiceUnavailable\|503" <<<"$OUT" \
  || fail "读取错误不是 503（副本不足应回 ServiceUnavailable）: ${OUT}"

if OUT="$(aws_at 0 s3api put-object --bucket "$BUCKET" --key unreachable.bin \
  --body "$TMP_DIR/small.bin" 2>&1)"; then
  fail "只剩 1 个节点时写入应失败，实际成功"
fi
grep -q "ServiceUnavailable\|503" <<<"$OUT" \
  || fail "写入错误不是 503: ${OUT}"

echo
echo "PASS: 集群冒烟通过（${N} 节点，N=3 W=2 R=2）"
