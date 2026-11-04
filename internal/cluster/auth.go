package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 内部协议的认证头。
//
// 选择 HMAC 共享密钥而不是 mTLS：边端集群的证书轮换与运维成本远高于
// 一个 32 字节密钥，而内部端口本身只在可信内网暴露（见 08 文档）。
// 代价是密钥泄露即全集群可伪造请求 —— 所以密钥必须来自 Secret、
// 不得写入日志，且 9100 端口必须由 NetworkPolicy 限制在同一 namespace。
const (
	HeaderNode = "X-Blink-Node"
	HeaderTime = "X-Blink-Time"
	HeaderSign = "X-Blink-Sign"

	// AuthSkew 是允许的时钟偏移，同时也是重放窗口。
	//
	// 窗口内同一请求可以被重放 —— 内部协议的所有写操作都是幂等的
	// （块按内容寻址、meta 按版本拒绝旧值），这是不做 nonce 缓存的前提。
	AuthSkew = 5 * time.Minute

	// MaxRequestBody 是内部请求体上限。
	//
	// 当前所有内部请求体都 ≤ block-size（4 MiB）或小 JSON；
	// 16 MiB 留出 EC shard 与未来协议的余量，同时防止一个
	// 未认证的请求把内存打满。
	MaxRequestBody = 16 << 20
)

var (
	// ErrUnauthenticated 表示内部请求认证失败。
	ErrUnauthenticated = errors.New("内部请求认证失败")
	// ErrBodyTooLarge 表示内部请求体超过 MaxRequestBody。
	ErrBodyTooLarge = errors.New("内部请求体过大")
)

// canonicalString 是签名的规范串：method|path|time|body_hash。
//
// 四段缺一不可：不签 method/path 会让"把一个合法签名挪到另一个接口"成立；
// 不签 body hash 会让中间人替换块内容而签名仍然有效。
func canonicalString(method, path, timestamp, digest string) string {
	return method + "|" + path + "|" + timestamp + "|" + digest
}

// digestOf 返回请求体的 SHA-256 十六进制摘要。
func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// sign 计算内部请求签名。
//
// 使用 hex 而不是 base64：hex 只含可打印字符，放进 HTTP 头与日志都安全，
// 长度增加 33% 对 32 字节的签名影响可以忽略。
func sign(secret []byte, method, path, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = io.WriteString(mac, canonicalString(method, path, timestamp, digestOf(body)))
	return hex.EncodeToString(mac.Sum(nil))
}

// SignRequest 为 req 写入三个认证头。
//
// body 必须与实际发送的请求体逐字节一致（包括 nil 与空切片等价）。
// 调用方负责在请求发出前调用；now 由调用方提供以便测试。
func SignRequest(req *http.Request, secret []byte, nodeID string, body []byte, now time.Time) {
	ts := strconv.FormatInt(now.Unix(), 10)
	req.Header.Set(HeaderNode, nodeID)
	req.Header.Set(HeaderTime, ts)
	req.Header.Set(HeaderSign, sign(secret, req.Method, requestPath(req), ts, body))
}

// VerifyRequest 校验内部请求，成功时返回节点 ID 与已读入的请求体。
//
// 返回的 body 是完整内容，调用方应把 r.Body 替换为它（Server 的中间件
// 已经这样做了），这样签名校验与业务处理看到的是同一份字节。
//
// allowed 为 nil 时跳过成员检查；非 nil 时只接受成员表中的节点 ——
// 持有正确密钥但不在成员表中的请求不应被当作合法节点。
func VerifyRequest(r *http.Request, secret []byte, allowed func(string) bool, now time.Time) (string, []byte, error) {
	node := strings.TrimSpace(r.Header.Get(HeaderNode))
	if err := validateNodeID(node); err != nil {
		return "", nil, fmt.Errorf("%w: %s 头无效", ErrUnauthenticated, HeaderNode)
	}

	tsStr := strings.TrimSpace(r.Header.Get(HeaderTime))
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %s 头缺失或不是 Unix 秒", ErrUnauthenticated, HeaderTime)
	}
	if skew := now.Sub(time.Unix(ts, 0)); skew > AuthSkew || skew < -AuthSkew {
		return "", nil, fmt.Errorf("%w: 时间戳超出 ±%s 重放窗口", ErrUnauthenticated, AuthSkew)
	}

	// 先在 Content-Length 上做快速拒绝，避免为明显超限的请求分配内存。
	if r.ContentLength > MaxRequestBody {
		return "", nil, fmt.Errorf("%w: Content-Length %d 超过上限 %d", ErrBodyTooLarge, r.ContentLength, MaxRequestBody)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxRequestBody+1))
	if err != nil {
		return "", nil, fmt.Errorf("%w: 读取请求体失败", ErrUnauthenticated)
	}
	if len(body) > MaxRequestBody {
		return "", nil, fmt.Errorf("%w: 请求体超过上限 %d", ErrBodyTooLarge, MaxRequestBody)
	}

	// 成员检查放在签名校验之前：未知节点无论签名是否正确都不该进入业务路径，
	// 同时也避免为陌生节点做无谓的 HMAC 计算。
	if allowed != nil && !allowed(node) {
		return "", nil, fmt.Errorf("%w: 节点 %q 不在成员表中", ErrUnauthenticated, node)
	}

	got := strings.TrimSpace(r.Header.Get(HeaderSign))
	if got == "" {
		return "", nil, fmt.Errorf("%w: 缺少 %s 头", ErrUnauthenticated, HeaderSign)
	}
	want := sign(secret, r.Method, requestPath(r), tsStr, body)
	// 常量时间比较：普通字符串比较会在首个不同字节处返回，
	// 理论上可被用来逐字节猜测签名。
	if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return "", nil, fmt.Errorf("%w: 签名不匹配", ErrUnauthenticated)
	}
	return node, body, nil
}

// requestPath 返回参与签名的路径。
//
// 用 EscapedPath 而不是 Path：Path 已被 net/http 百分号解码，
// 两个不同的原始路径可能解码成同一个 Path，签名就会失去区分能力。
// 根路径在部分请求中为空，统一按 "/" 处理。
func requestPath(r *http.Request) string {
	if p := r.URL.EscapedPath(); p != "" {
		return p
	}
	return "/"
}
