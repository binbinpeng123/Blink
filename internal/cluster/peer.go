package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// maxResponseBytes 是内部响应体的读取上限。
//
// 块数据走独立的流式接口，这里只处理 JSON（健康检查、元数据），
// 因此 16 MiB 足够且能防住一个错误的实现对端把内存撑爆。
const maxResponseBytes = 16 << 20

// PeerClient 是到单个远端节点的内部协议客户端。
//
// 每个 Peer 持有自己的 http.Client：连接池按目标节点隔离，
// 一个节点不可用不会拖慢其他节点的连接复用。
type PeerClient struct {
	peer   Peer
	selfID string
	secret []byte
	client *http.Client
}

// NewPeerClient 构造到 p 的客户端。
//
// selfID 与 secret 用于给每个请求签名；timeout<=0 时使用默认值。
func NewPeerClient(p Peer, selfID string, secret []byte, timeout time.Duration) *PeerClient {
	if timeout <= 0 {
		timeout = DefaultPeerTimeout
	}
	return &PeerClient{
		peer:   p,
		selfID: selfID,
		secret: secret,
		client: &http.Client{
			Timeout: timeout,
			// 内部协议没有重定向语义。跟随重定向会让签名路径与实际路径不一致，
			// 结果是一个难以理解的 401；直接把 3xx 当最终响应更诚实。
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Peer 返回客户端对应的成员。
func (c *PeerClient) Peer() Peer {
	return c.peer
}

// Health 调用远端 /internal/v1/health。
func (c *PeerClient) Health(ctx context.Context) (Health, error) {
	var out Health
	if err := c.do(ctx, http.MethodGet, "/internal/v1/health", nil, &out); err != nil {
		return Health{}, err
	}
	return out, nil
}

// do 发送一个已签名的内部请求，成功时把 JSON 响应解码到 out。
func (c *PeerClient) do(ctx context.Context, method, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+c.peer.Addr+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("节点 %s: 构造请求 %s %s 失败: %w", c.peer.ID, method, path, err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	SignRequest(req, c.secret, c.selfID, body, time.Now())

	resp, err := c.client.Do(req)
	if err != nil {
		// err 来自 net/http，只含地址与网络错误，不含密钥。
		return fmt.Errorf("节点 %s: %s %s 失败: %w", c.peer.ID, method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &PeerError{
			Peer:    c.peer.ID,
			Method:  method,
			Path:    path,
			Status:  resp.StatusCode,
			Message: strings.TrimSpace(string(snippet)),
		}
	}

	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("节点 %s: 读取 %s 响应失败: %w", c.peer.ID, path, err)
	}
	if len(data) > maxResponseBytes {
		return fmt.Errorf("节点 %s: %s 响应超过 %d 字节上限", c.peer.ID, path, maxResponseBytes)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("节点 %s: 解析 %s 响应失败: %w", c.peer.ID, path, err)
	}
	return nil
}

// PeerError 表示远端返回了非 200 状态码。
//
// 单独的类型让上层能区分"节点不可达"（网络错误）与"节点明确拒绝"，
// 两者在法定人数统计里的处理不同：前者计入失败，后者还要看状态码
// 决定是否值得重试。
type PeerError struct {
	Peer    string
	Method  string
	Path    string
	Status  int
	Message string
}

func (e *PeerError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("节点 %s: %s %s 返回 %d", e.Peer, e.Method, e.Path, e.Status)
	}
	return fmt.Sprintf("节点 %s: %s %s 返回 %d: %s", e.Peer, e.Method, e.Path, e.Status, e.Message)
}
