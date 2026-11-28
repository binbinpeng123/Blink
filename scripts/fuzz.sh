#!/usr/bin/env bash
# 协议解析层的本地模糊测试：逐个目标有界运行。
#
# 不启动服务、不写数据目录、不联网 —— 只解析内存里的字符串，
# 因此不受"本机压测写入 ≤ 512 MiB"约束。
#
# 用法：
#   scripts/fuzz.sh                      # 每个目标 20 秒
#   scripts/fuzz.sh 60                   # 每个目标 60 秒
#   scripts/fuzz.sh 20 FuzzParseRange FuzzParseChunkHeader   # 只跑指定目标
#
# 与 go test 的分工：种子语料随 `go test ./...` 每次执行（毫秒级）；
# 本脚本负责持续随机探索。某个目标发现失败时会自动写入
# testdata/fuzz/<目标>/<哈希>，此后 `go test ./<包>` 会把它当作回归用例。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FUZZTIME="${1:-20}"
shift || true
WANTED=("$@")
if ! [[ "$FUZZTIME" =~ ^[0-9]+$ ]]; then
  echo "用法：$0 [每目标秒数] [目标名...]" >&2
  exit 2
fi

# 包:目标 —— 新增 Fuzz 目标时必须同步登记，避免"写了却从没跑过"。
ALL=(
  internal/s3api:FuzzParseRange
  internal/s3api:FuzzParseCopyPartRange
  internal/s3api:FuzzS3URLEncode
  internal/s3api:FuzzDecodeListToken
  internal/s3api:FuzzParseTrailerNames
  internal/s3api:FuzzParseCopySource
  internal/s3api:FuzzParseContentMD5
  internal/s3api:FuzzParseDecodedContentLength
  internal/s3api:FuzzDecodeCORSConfiguration
  internal/s3api:FuzzCorsPatternMatches
  internal/s3api:FuzzSplitHeaderList
  internal/s3api:FuzzParseListParams
  internal/s3api:FuzzParsePartNumber
  internal/s3api:FuzzParsePostPolicy
  internal/sigv4:FuzzCanonicalQueryString
  internal/sigv4:FuzzParseChunkHeader
  internal/sigv4:FuzzParseTrailerLine
  internal/sigv4:FuzzParseAuthorization
  internal/store:FuzzValidateBucketName
  internal/store:FuzzParsePartFileName
  internal/store:FuzzDecodeObjectMeta
  internal/store:FuzzDecodePartState
)

selected=()
if [[ ${#WANTED[@]} -eq 0 ]]; then
  selected=("${ALL[@]}")
else
  for want in "${WANTED[@]}"; do
    hit=""
    for entry in "${ALL[@]}"; do
      if [[ "${entry#*:}" == "$want" ]]; then
        hit="$entry"
        break
      fi
    done
    if [[ -z "$hit" ]]; then
      echo "未登记的目标：$want（可选：$(printf '%s ' "${ALL[@]#*:}")）" >&2
      exit 2
    fi
    selected+=("$hit")
  done
fi

for entry in "${selected[@]}"; do
  pkg="${entry%%:*}"
  target="${entry#*:}"
  echo "===== $target ($pkg, ${FUZZTIME}s)"
  go test -run '^$' -fuzz "^${target}$" -fuzztime="${FUZZTIME}s" "./$pkg"
  echo
done

echo "全部目标通过（每个 ${FUZZTIME}s，仅供参考：有界时长不构成完备性证明）。"