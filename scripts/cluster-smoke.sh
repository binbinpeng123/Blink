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
#   - 跨节点分片上传：建会话/传分片/列举/完成可以落在不同节点，
#     完成后任意节点读回逐字节一致，且会话不残留；
#   - 有进行中 MPU 的桶删除被拒，Abort 后可以删除；
#   - 非空桶删除被拒；清空后删桶对所有节点生效；
#   - 杀掉 1 个节点后仍可读写（W=2 / R=2 仍可满足）；
#   - 节点用同一数据目录重启后，宕机期间的写入被自动修复补齐，
#     且修复队列（hint 文件）随任务完成清空；
#   - 只保留 1 个节点时读写回 503，而不是假装对象不存在。
#
# 带 --ec 时额外验证纠删码：
#   - 写入 ≥32 MiB 对象，其它节点读回逐字节一致；
#   - meta 记录 layout=ec / 4+2 分片组；
#   - 删掉 2 片后仍能从剩余 4 片重建读取；
#   - 读路径触发的修复把删掉的分片重新写回磁盘。
#
# 该脚本只用固定端口、临时目录与测试凭据，仅用于本地验证，
# 不得指向任何真实部署。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

N="${1:-3}"
MODE="${2:-}"
case "$N:$MODE" in
  3:) ;;
  6:--ec) ;;
  *)
    echo "用法: $0 [3 | 6 --ec]" >&2
    echo "3 节点验证多副本读写；6 节点 --ec 额外验证纠删码（4+2）。" >&2
    exit 2
    ;;
esac
if [[ "$N" == "6" ]]; then
  EC_ENABLED=true
  EC_K=4
  EC_M=2
else
  EC_ENABLED=false
  EC_K=0
  EC_M=0
fi

BIN="bin/blink"
# 最后一个节点的下标：脚本里"另一个节点"的验证尽量落在它身上，
# 既覆盖非首节点的视角，也能在 3/6 两种规模下复用同一份逻辑。
LAST=$((N - 1))
BUCKET="cluster-smoke-$(date +%s)"
S3_PORT_BASE=19101
INTERNAL_PORT_BASE=19201
PEER_TIMEOUT="500ms"
# 修复相关的默认值（30s 兜底唤醒、24h 反熵）对冒烟测试太慢：这里压到
# 亚秒级，让"节点重启后自动补齐"能在几秒内观察到。hint TTL 放宽是为了
# 避免故障验证过程中任务被提前丢弃，掩盖真正的失败。
REPAIR_INTERVAL="500ms"
HINT_TTL="10m"
SCRUB_INTERVAL="-1s"

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

# start_node <序号>：启动一个节点。始终复用 $TMP_DIR/node<i>/data，
# 这样"宕机 → 用同一数据目录重启"不会丢掉节点本地的副本与 hint。
start_node() {
  local i="$1"
  mkdir -p "$TMP_DIR/node${i}/data"
  BLINK_CLUSTER_MODE=cluster \
  BLINK_NODE_ID="node$((i + 1))" \
  BLINK_CLUSTER_PEERS="$PEERS" \
  BLINK_INTERNAL_ADDR="127.0.0.1:$(internal_port "$i")" \
  BLINK_CLUSTER_SECRET="$CLUSTER_SECRET" \
  BLINK_REPLICA_COUNT=3 \
  BLINK_WRITE_QUORUM=2 \
  BLINK_READ_QUORUM=2 \
  BLINK_EC_ENABLED="$EC_ENABLED" \
  BLINK_EC_K="$EC_K" \
  BLINK_EC_M="$EC_M" \
    "$BIN" \
      --addr "127.0.0.1:$(s3_port "$i")" \
      --data-dir "$TMP_DIR/node${i}/data" \
      --access-key "$AWS_ACCESS_KEY_ID" \
      --secret-key "$AWS_SECRET_ACCESS_KEY" \
      --log-level debug \
      --verify-read \
      --peer-timeout "$PEER_TIMEOUT" \
      --repair-interval "$REPAIR_INTERVAL" \
      --hint-ttl "$HINT_TTL" \
      --scrub-interval "$SCRUB_INTERVAL" \
      >>"$TMP_DIR/node${i}.log" 2>&1 &
  PIDS[i]="$!"
}

# wait_node_ready <序号>：健康检查就绪才继续，避免把启动竞态误判为故障。
wait_node_ready() {
  local i="$1"
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:$(s3_port "$i")/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

echo "==> 构建"
make build >/dev/null

echo "==> 启动 ${N} 个节点"
for i in $(seq 0 $((N - 1))); do
  start_node "$i"
done

echo "==> 等待节点就绪"
for i in $(seq 0 $((N - 1))); do
  wait_node_ready "$i" || fail "节点 $((i + 1)) 未在 10 秒内就绪"
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
if OUT="$(aws_at "$LAST" s3api create-bucket --bucket "$BUCKET" 2>&1)"; then
  fail "在节点 ${N} 重复建桶应失败，实际成功"
fi
grep -q "BucketAlreadyOwnedByYou" <<<"$OUT" \
  || fail "重复建桶错误不是 BucketAlreadyOwnedByYou: ${OUT}"

echo "==> 生成测试对象"
PAYLOAD="$TMP_DIR/payload.bin"
dd if=/dev/urandom of="$PAYLOAD" bs=1024 count=8192 2>/dev/null # 8 MiB，跨多个块
WANT_ETAG="$(md5 -q "$PAYLOAD")"
WANT_SIZE="$(wc -c <"$PAYLOAD" | tr -d ' ')"

echo "==> 节点 1 写入，其余节点读取"
aws_at 0 s3api put-object --bucket "$BUCKET" --key big.bin --body "$PAYLOAD" >/dev/null \
  || fail "节点 1 写入失败"

for i in $(seq 1 "$LAST"); do
  aws_at "$i" s3api get-object --bucket "$BUCKET" --key big.bin "$TMP_DIR/read${i}.bin" >/dev/null \
    || fail "节点 $((i + 1)) 读取失败"
  cmp -s "$PAYLOAD" "$TMP_DIR/read${i}.bin" \
    || fail "节点 $((i + 1)) 读到的内容与写入不一致"
done

echo "==> 校验 HEAD 元数据"
HEAD_ETAG="$(aws_at "$LAST" s3api head-object --bucket "$BUCKET" --key big.bin --query ETag --output text \
  | tr -d '"')"
[[ "$HEAD_ETAG" == "$WANT_ETAG" ]] || fail "HEAD ETag=${HEAD_ETAG}，期望 ${WANT_ETAG}"
HEAD_SIZE="$(aws_at "$LAST" s3api head-object --bucket "$BUCKET" --key big.bin --query ContentLength --output text)"
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
aws_at "$LAST" s3api put-object --bucket "$BUCKET" --key small.bin --body "$PAYLOAD" >/dev/null \
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
for i in $(seq 0 "$LAST"); do
  aws_at "$i" s3api put-object --bucket "$BUCKET" --key "dir/obj-$i.bin" \
    --body "$TMP_DIR/small.bin" >/dev/null || fail "节点 $((i + 1)) 写入 dir/obj-$i.bin 失败"
done

list_keys() {
  # $1 = 节点序号；过滤掉空结果时 aws-cli 输出的 None。
  aws_at "$1" s3api list-objects-v2 --bucket "$BUCKET" \
    --query 'Contents[].Key' --output text 2>/dev/null \
    | tr '\t' '\n' | grep -vx 'None' | sed '/^$/d' | sort
}
WANT_KEYS="$({ printf '%s\n' big.bin small.bin
  for i in $(seq 0 "$LAST"); do printf 'dir/obj-%s.bin\n' "$i"; done; } | sort)"
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

# 分片上传的三个阶段刻意由不同节点协调：会话位置由 uploadId 的哈希决定，
# 与"谁创建"无关；任何一个阶段绑死协调者都会在这里暴露。
echo "==> 跨节点分片上传（MPU）"
MPU_BUCKET="${BUCKET}-mpu"
MPU_KEY="dir/mpu.bin"
aws_at 1 s3api create-bucket --bucket "$MPU_BUCKET" >/dev/null \
  || fail "建桶 ${MPU_BUCKET} 失败"

UPLOAD_ID="$(aws_at 0 s3api create-multipart-upload --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
  --query UploadId --output text)" || fail "节点 1 创建分片上传失败"
[[ "$UPLOAD_ID" =~ ^[0-9a-f]{32}$ ]] || fail "uploadId 格式不符: ${UPLOAD_ID}"

# 非末分片必须 ≥5 MiB；第二片不足 5 MiB 作为末分片。
dd if=/dev/urandom of="$TMP_DIR/part1.bin" bs=1048576 count=5 2>/dev/null
dd if=/dev/urandom of="$TMP_DIR/part2.bin" bs=1048576 count=1 2>/dev/null
dd if=/dev/urandom bs=1 count=12345 >>"$TMP_DIR/part2.bin" 2>/dev/null
cat "$TMP_DIR/part1.bin" "$TMP_DIR/part2.bin" >"$TMP_DIR/mpu.want"

# aws-cli 的 ETag 输出自带双引号，可以直接嵌进 JSON。
ETAG1="$(aws_at 1 s3api upload-part --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
  --upload-id "$UPLOAD_ID" --part-number 1 --body "$TMP_DIR/part1.bin" \
  --query ETag --output text)" || fail "节点 2 上传分片 1 失败"
ETAG2="$(aws_at "$LAST" s3api upload-part --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
  --upload-id "$UPLOAD_ID" --part-number 2 --body "$TMP_DIR/part2.bin" \
  --query ETag --output text)" || fail "节点 ${N} 上传分片 2 失败"

LIST_PARTS="$(aws_at 0 s3api list-parts --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
  --upload-id "$UPLOAD_ID" --query 'Parts[].[PartNumber,Size]' --output text)" \
  || fail "节点 1 列举分片失败"
EXPECTED_PARTS="$(printf '1\t%s\n2\t%s' "$(wc -c <"$TMP_DIR/part1.bin" | tr -d ' ')" \
  "$(wc -c <"$TMP_DIR/part2.bin" | tr -d ' ')")"
[[ "$LIST_PARTS" == "$EXPECTED_PARTS" ]] \
  || fail "分片列表不符\n期望:\n${EXPECTED_PARTS}\n实际:\n${LIST_PARTS}"

# 会话列表必须扇形到全部节点后归并，任一节点漏答都会少一条。
for i in $(seq 0 $((N - 1))); do
  aws_at "$i" s3api list-multipart-uploads --bucket "$MPU_BUCKET" \
    --query 'Uploads[].UploadId' --output text 2>/dev/null \
    | tr '\t' '\n' | grep -qx "$UPLOAD_ID" \
    || fail "节点 $((i + 1)) 的 list-multipart-uploads 缺少 ${UPLOAD_ID}"
done

cat >"$TMP_DIR/complete.json" <<JSON
{"Parts":[{"ETag":${ETAG1},"PartNumber":1},{"ETag":${ETAG2},"PartNumber":2}]}
JSON
# 由节点 2 完成：既不是建会话的节点，也不是末分片的协调者。
aws_at 1 s3api complete-multipart-upload --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
  --upload-id "$UPLOAD_ID" --multipart-upload "file://$TMP_DIR/complete.json" >/dev/null \
  || fail "Complete 失败"

# S3 的多分片 ETag = md5(各分片 ETag 的原始 MD5 字节) + "-<分片数>"。
WANT_MPU_ETAG="$({ md5 -q "$TMP_DIR/part1.bin" | xxd -r -p; md5 -q "$TMP_DIR/part2.bin" | xxd -r -p; } \
  | md5 -q)-2"
for i in $(seq 0 $((N - 1))); do
  aws_at "$i" s3api get-object --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
    "$TMP_DIR/mpu-${i}.got" >/dev/null || fail "节点 $((i + 1)) 读取 MPU 对象失败"
  cmp -s "$TMP_DIR/mpu.want" "$TMP_DIR/mpu-${i}.got" \
    || fail "节点 $((i + 1)) 读到的 MPU 对象内容不一致"
  GOT_MPU_ETAG="$(aws_at "$i" s3api head-object --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
    --query ETag --output text | tr -d '"')"
  [[ "$GOT_MPU_ETAG" == "$WANT_MPU_ETAG" ]] \
    || fail "节点 $((i + 1)) 的 MPU ETag=${GOT_MPU_ETAG}，期望 ${WANT_MPU_ETAG}"
done

# Complete 后会话必须消失，否则后续 create 会撞上陈旧状态。
for i in $(seq 0 $((N - 1))); do
  if OUT="$(aws_at "$i" s3api list-parts --bucket "$MPU_BUCKET" --key "$MPU_KEY" \
    --upload-id "$UPLOAD_ID" 2>&1)"; then
    fail "节点 $((i + 1)) 在 Complete 后仍能列举已完成的分片"
  fi
  grep -q "NoSuchUpload" <<<"$OUT" || fail "Completed 后 list-parts 错误不是 NoSuchUpload: ${OUT}"
done

# 进行中的 MPU 让桶"非空"：非空判定必须查全部节点，而不只是本地磁盘。
echo "==> 进行中 MPU 阻止删桶，Abort 后可删"
UPLOAD_ID2="$(aws_at "$LAST" s3api create-multipart-upload --bucket "$MPU_BUCKET" --key in-progress.bin \
  --query UploadId --output text)" || fail "创建第二个会话失败"
if OUT="$(aws_at 0 s3api delete-bucket --bucket "$MPU_BUCKET" 2>&1)"; then
  fail "有进行中 MPU 时删桶应失败，实际成功"
fi
grep -q "BucketNotEmpty" <<<"$OUT" || fail "有进行中 MPU 时删桶错误不是 BucketNotEmpty: ${OUT}"

aws_at 1 s3api abort-multipart-upload --bucket "$MPU_BUCKET" --key in-progress.bin \
  --upload-id "$UPLOAD_ID2" >/dev/null || fail "Abort 失败"
if OUT="$(aws_at "$LAST" s3api list-parts --bucket "$MPU_BUCKET" --key in-progress.bin \
  --upload-id "$UPLOAD_ID2" 2>&1)"; then
  fail "Abort 后仍能列举分片"
fi
grep -q "NoSuchUpload" <<<"$OUT" || fail "Abort 后 list-parts 错误不是 NoSuchUpload: ${OUT}"

# Abort 是集群范围的：会话清掉后桶里只剩已完成的 mpu.bin，删对象即可删桶。
aws_at 0 s3api delete-object --bucket "$MPU_BUCKET" --key "$MPU_KEY" >/dev/null \
  || fail "删除 MPU 对象失败"
aws_at "$LAST" s3api delete-bucket --bucket "$MPU_BUCKET" >/dev/null \
  || fail "Abort 后删桶失败"
for i in $(seq 0 $((N - 1))); do
  if aws_at "$i" s3api head-bucket --bucket "$MPU_BUCKET" >/dev/null 2>&1; then
    fail "节点 $((i + 1)) 删除后仍能看到桶 ${MPU_BUCKET}"
  fi
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
aws_at "$LAST" s3api put-object --bucket "$META_BUCKET" --key keep.bin \
  --body "$TMP_DIR/small.bin" >/dev/null || fail "写入 ${META_BUCKET}/keep.bin 失败"

if OUT="$(aws_at 0 s3api delete-bucket --bucket "$META_BUCKET" 2>&1)"; then
  fail "非空桶删除应失败，实际成功"
fi
grep -q "BucketNotEmpty" <<<"$OUT" || fail "非空桶删除错误不是 BucketNotEmpty: ${OUT}"

aws_at 0 s3api delete-object --bucket "$META_BUCKET" --key keep.bin >/dev/null \
  || fail "删除 ${META_BUCKET}/keep.bin 失败"
aws_at "$LAST" s3api delete-bucket --bucket "$META_BUCKET" >/dev/null \
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

# EC 段落在所有节点都在线时执行：4+2 的编码写入要求 6 个组成员全部可达，
# 而下面的故障注入会杀掉一个节点。
if [[ "$EC_ENABLED" == "true" ]]; then
  echo "==> 纠删码：写入 40 MiB 对象（超过 32 MiB 阈值）"
  EC_PAYLOAD="$TMP_DIR/ec.bin"
  dd if=/dev/urandom of="$EC_PAYLOAD" bs=1048576 count=40 2>/dev/null
  aws_at 0 s3api put-object --bucket "$BUCKET" --key ec/object.bin --body "$EC_PAYLOAD" >/dev/null \
    || fail "EC 对象写入失败"
  aws_at "$LAST" s3api get-object --bucket "$BUCKET" --key ec/object.bin "$TMP_DIR/ec.got" >/dev/null \
    || fail "EC 对象跨节点读取失败"
  cmp -s "$EC_PAYLOAD" "$TMP_DIR/ec.got" \
    || fail "EC 对象跨节点读回不一致"

  echo "==> 校验 EC meta 与分片布局"
  EC_META="$(grep -rlF '"key":"ec/object.bin"' "$TMP_DIR"/node*/data/buckets | head -1 || true)"
  [[ -n "$EC_META" ]] || fail "找不到 EC 对象的 meta"
  # 依次输出 layout、k、m、k+m 个组员、首块 k+m 个分片哈希，各占一行。
  # 不用 mapfile：macOS 自带 bash 3.2 没有它，而这些脚本要在开发机上跑。
  EC_LINES=()
  while IFS= read -r ec_line; do
    EC_LINES+=("$ec_line")
  done < <(python3 - "$EC_META" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
ec = m.get("ec") or {}
print(m.get("layout", ""))
print(ec.get("k", ""))
print(ec.get("m", ""))
for gid in ec.get("group", []):
    print(gid)
for s in m["blocks"][0].get("shards", []):
    print(s["hash"])
PY
)
  [[ "${#EC_LINES[@]}" -eq 15 ]] \
    || fail "EC meta 解析出 ${#EC_LINES[@]} 行，期望 15 行（layout/k/m/6 组员/6 分片）"
  [[ "${EC_LINES[0]}" == "ec" ]] || fail "EC 对象 layout=${EC_LINES[0]}，期望 ec"
  [[ "${EC_LINES[1]}" == "4" && "${EC_LINES[2]}" == "2" ]] \
    || fail "EC 参数=${EC_LINES[1]}+${EC_LINES[2]}，期望 4+2"
  EC_GROUP=("${EC_LINES[@]:3:6}")
  EC_SHARDS=("${EC_LINES[@]:9:6}")

  echo "==> 删掉 2 个数据片，仍从剩余 4 片重建读取"
  declare -a EC_SHARD_FILE=()
  for idx in 0 1; do
    owner="${EC_GROUP[$idx]}"
    node_idx=$(( ${owner#node} - 1 ))
    hash="${EC_SHARDS[$idx]}"
    ec_file="$TMP_DIR/node${node_idx}/data/cas/${hash:0:2}/${hash}"
    [[ -f "$ec_file" ]] || fail "分片 ${idx} 不在 ${owner} 的 CAS 目录：${ec_file}"
    rm -f "$ec_file"
    EC_SHARD_FILE[$idx]="$ec_file"
  done
  aws_at 1 s3api get-object --bucket "$BUCKET" --key ec/object.bin "$TMP_DIR/ec.rebuilt" >/dev/null \
    || fail "删 2 片后 EC 对象读取失败"
  cmp -s "$EC_PAYLOAD" "$TMP_DIR/ec.rebuilt" \
    || fail "删 2 片后重建的内容与写入不一致"

  # 读路径会把缺片交给修复队列；分片应被重建回原节点。
  EC_REPAIR_DEADLINE=$((SECONDS + 60))
  while :; do
    ec_missing=0
    for idx in 0 1; do
      if [[ ! -f "${EC_SHARD_FILE[$idx]}" ]]; then
        ec_missing=1
      fi
    done
    if (( ec_missing == 0 )); then
      break
    fi
    (( SECONDS < EC_REPAIR_DEADLINE )) || fail "删片后 60 秒内未重建分片"
    sleep 0.2
  done
fi

echo "==> 杀掉 1 个节点后仍可读写"
DEAD=$LAST
kill -9 "${PIDS[DEAD]}" 2>/dev/null || true
wait "${PIDS[DEAD]}" 2>/dev/null || true
PIDS[DEAD]=""

aws_at 0 s3api put-object --bucket "$BUCKET" --key degraded.bin --body "$TMP_DIR/small.bin" >/dev/null \
  || fail "1 个节点宕机后写入失败（W=2 应仍可满足）"
aws_at 1 s3api get-object --bucket "$BUCKET" --key degraded.bin "$TMP_DIR/degraded.got" >/dev/null \
  || fail "1 个节点宕机后读取失败（R=2 应仍可满足）"
cmp -s "$TMP_DIR/small.bin" "$TMP_DIR/degraded.got" \
  || fail "1 个节点宕机后读到的内容不一致"

# 用全新随机内容写一份：块在集群里从未出现过，修复必须真的搬运数据，
# 而不是靠 CAS 去重"碰巧"命中已有块。
OUTAGE_PAYLOAD="$TMP_DIR/outage.bin"
dd if=/dev/urandom of="$OUTAGE_PAYLOAD" bs=1024 count=1024 2>/dev/null

# 副本集按 key 哈希选出 3 个节点，宕机节点未必在其中。逐个写候选 key，
# 用"存活节点上的 meta 份数"反推：宕机节点是副本目标时只会有 REPLICA_COUNT-1
# 份 meta 落盘（写不进去的那份进了 hint），否则 3 份齐全。选中第一个命中
# 的 key 作为修复断言的样本，3 节点与 6 节点因此共用同一份逻辑。
REPLICA_COUNT=3
OUTAGE_KEY=""
for j in $(seq 0 7); do
  key="outage-${j}.bin"
  aws_at 0 s3api put-object --bucket "$BUCKET" --key "$key" --body "$OUTAGE_PAYLOAD" >/dev/null \
    || fail "1 个节点宕机后写入 ${key} 失败"
  holders=0
  for i in $(seq 0 "$LAST"); do
    if [[ "$i" == "$DEAD" ]]; then
      continue
    fi
    if grep -rqF "\"key\":\"${key}\"" "$TMP_DIR/node${i}/data/buckets" 2>/dev/null; then
      holders=$((holders + 1))
    fi
  done
  if (( holders == REPLICA_COUNT - 1 )); then
    OUTAGE_KEY="$key"
    break
  fi
done
[[ -n "$OUTAGE_KEY" ]] || fail "8 次尝试内未选到副本集包含宕机节点的 key"

# 宕机节点此刻不该有这份副本——否则后面的"修复补齐"断言没有意义。
if grep -rqF "\"key\":\"${OUTAGE_KEY}\"" "$TMP_DIR/node${DEAD}/data/buckets" 2>/dev/null; then
  fail "宕机节点不应持有宕机期间写入的副本"
fi

# 删桶与对象数据面相反：任一节点不可达就拒绝，否则宕机节点可能带着
# 旧桶（与旧对象）回归，造出"桶删了却还能读到数据"的状态。
if OUT="$(aws_at 0 s3api delete-bucket --bucket "$BUCKET" 2>&1)"; then
  fail "节点宕机时删桶应失败，实际成功"
fi
grep -q "ServiceUnavailable\|503" <<<"$OUT" \
  || fail "节点宕机时删桶错误不是 503: ${OUT}"

echo "==> 重启节点 ${N}，验证自动修复补齐"
# 同一数据目录 + 同一节点 ID：模拟 Pod 原地重启（StatefulSet 的常态）。
start_node "$DEAD"
wait_node_ready "$DEAD" || fail "节点 ${N} 重启后未在 10 秒内就绪"

# pending_hints：任一节点上仍有未完成的修复任务就输出文件名。
pending_hints() {
  find "$TMP_DIR"/node*/data/cluster/hints -name '*.json' -print -quit 2>/dev/null
}

# 修复是异步的：轮询"副本出现且 hint 清空"，固定 sleep 会在慢机器上
# 随机失败。
REPAIR_DEADLINE=$((SECONDS + 30))
while :; do
  if grep -rqF "\"key\":\"${OUTAGE_KEY}\"" "$TMP_DIR/node${DEAD}/data/buckets" 2>/dev/null \
    && [[ -z "$(pending_hints)" ]]; then
    break
  fi
  ((SECONDS < REPAIR_DEADLINE)) || fail "节点 ${N} 重启 30 秒后仍未补齐副本/清空 hint"
  sleep 0.1
done

# 不只核对 meta：块也必须真的落到重启节点上。读路径会从远端兜底，
# 只读一次无法区分"本地已修复"和"每次都走远端搬块"。
META_FILE="$(grep -rlF "\"key\":\"${OUTAGE_KEY}\"" "$TMP_DIR/node${DEAD}/data/buckets" | head -1 || true)"
for hash in $(grep -o '"hash":"[0-9a-f]\{64\}"' "$META_FILE" | cut -d'"' -f4); do
  [[ -f "$TMP_DIR/node${DEAD}/data/cas/${hash:0:2}/$hash" ]] \
    || fail "重启节点缺少修复块 ${hash}"
done

aws_at "$DEAD" s3api get-object --bucket "$BUCKET" --key "$OUTAGE_KEY" "$TMP_DIR/outage.got" >/dev/null \
  || fail "修复后节点 ${N} 读取 ${OUTAGE_KEY} 失败"
cmp -s "$OUTAGE_PAYLOAD" "$TMP_DIR/outage.got" \
  || fail "修复后节点 ${N} 读到的内容与写入不一致"

echo "==> 只留节点 1 后读响应返回 503"
for idx in $(seq 1 "$LAST"); do
  kill -9 "${PIDS[$idx]}" 2>/dev/null || true
  wait "${PIDS[$idx]}" 2>/dev/null || true
  PIDS[$idx]=""
done

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
if [[ "$EC_ENABLED" == "true" ]]; then
  echo "PASS: 集群冒烟通过（${N} 节点，N=3 W=2 R=2，EC 4+2 丢 2 片仍可读）"
else
  echo "PASS: 集群冒烟通过（${N} 节点，N=3 W=2 R=2）"
fi
