package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// maxResponseBytes 是内部 JSON 响应体的读取上限。
//
// 块数据走独立的流式接口，这里只处理 JSON（健康检查、元数据），
// 因此 16 MiB 足够且能防住一个错误的实现对端把内存撑爆。
const maxResponseBytes = 16 << 20

// blockTransferTimeout 是单个块上传/下载的超时。
//
// 不能沿用 PeerTimeout（2s）：那是给几十字节 JSON 用的，一个 4 MiB 块
// 在磁盘繁忙或链路拥塞时 2 秒完全可能不够。30s 远大于正常耗时，
// 只用于把"对端僵死"变成一个可统计的失败，而不是无限挂住协调者。
// 下载侧由调用方传入带超时的 context，因为一次读取可能包含多个块。
const blockTransferTimeout = 30 * time.Second

// PeerClient 是到单个远端节点的内部协议客户端。
//
// 每个 Peer 持有自己的 http.Client：连接池按目标节点隔离，
// 一个节点不可用不会拖慢其他节点的连接复用。
//
// 块传输用另一个 client：http.Client.Timeout 覆盖**整个响应体**的读取，
// 对可能持续数秒的大响应是错误的时间尺度。JSON 用小超时快速失败，
// 块传输用传输级超时 + Transport 的响应头超时分别约束两段。
type PeerClient struct {
	peer     Peer
	selfID   string
	secret   []byte
	client   *http.Client
	transfer *http.Client
}

// NewPeerClient 构造到 p 的客户端。
//
// selfID 与 secret 用于给每个请求签名；timeout<=0 时使用默认值。
func NewPeerClient(p Peer, selfID string, secret []byte, timeout time.Duration) *PeerClient {
	if timeout <= 0 {
		timeout = DefaultPeerTimeout
	}
	checkRedirect := func(*http.Request, []*http.Request) error {
		// 内部协议没有重定向语义。跟随重定向会让签名路径与实际路径不一致，
		// 结果是一个难以理解的 401；直接把 3xx 当最终响应更诚实。
		return http.ErrUseLastResponse
	}
	return &PeerClient{
		peer:   p,
		selfID: selfID,
		secret: secret,
		client: &http.Client{Timeout: timeout, CheckRedirect: checkRedirect},
		transfer: &http.Client{
			CheckRedirect: checkRedirect,
			Transport: &http.Transport{
				DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
				ResponseHeaderTimeout: timeout,
				IdleConnTimeout:       90 * time.Second,
				MaxIdleConnsPerHost:   8,
			},
		},
	}
}

// Peer 返回客户端对应的成员。
func (c *PeerClient) Peer() Peer {
	return c.peer
}

// closeIdle 关闭到该节点的空闲连接。
func (c *PeerClient) closeIdle() {
	c.client.CloseIdleConnections()
	c.transfer.CloseIdleConnections()
}

// Health 调用远端 /internal/v1/health。
func (c *PeerClient) Health(ctx context.Context) (Health, error) {
	var out Health
	if err := c.do(ctx, http.MethodGet, "/internal/v1/health", nil, &out); err != nil {
		return Health{}, err
	}
	return out, nil
}

// PutBlock 把一个完整内容块写到远端节点的 CAS。
//
// 本地已经算好 hash，对端仍会重算并校验；两边不一致时返回 400 PeerError，
// 调用方应把它当作这条链路的证据，而不是重试掩盖。
func (c *PeerClient) PutBlock(ctx context.Context, hash string, data []byte) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, blockTransferTimeout)
	defer cancel()

	path := pathBlock + "?" + url.Values{"hash": {hash}}.Encode()
	resp, err := c.doTransfer(ctx, http.MethodPost, path, data)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return false, c.peerError(http.MethodPost, path, resp)
	}
	var out struct {
		Deduped bool `json:"deduped"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<10)).Decode(&out); err != nil {
		return false, fmt.Errorf("节点 %s: 解析块写入响应失败: %w", c.peer.ID, err)
	}
	return out.Deduped, nil
}

// GetBlock 把远端块 b 的 [offset, offset+length) 字节流式写入 w。
//
// 返回的字节数必须恰好等于 length：Content-Length 已声明，短读意味着
// 对端中途出错或连接断开，此时绝不能把半截块当成成功交给上层。
//
// 请求里带上块表记录的长度与 CRC32C：对端据此做与本地读完全相同的
// 完整性校验（verifyOnRead 时），而不是信任自己在磁盘上看到的文件。
func (c *PeerClient) GetBlock(ctx context.Context, b store.BlockDesc, offset, length int64, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, blockTransferTimeout)
	defer cancel()

	q := url.Values{
		"hash":        {b.Hash},
		"blockLength": {strconv.FormatInt(b.Length, 10)},
		"crc":         {fmt.Sprintf("%08x", b.CRC32C)},
		"offset":      {strconv.FormatInt(offset, 10)},
		"length":      {strconv.FormatInt(length, 10)},
	}
	path := pathBlock + "?" + q.Encode()
	resp, err := c.doTransfer(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return c.peerError(http.MethodGet, path, resp)
	}

	n, err := io.CopyN(w, resp.Body, length)
	if err != nil {
		return fmt.Errorf("节点 %s: 读取块 %s 失败: 已收 %d/%d 字节: %w",
			c.peer.ID, b.Hash, n, length, err)
	}
	return nil
}

// GetMeta 读取远端节点上某对象的副本元数据。
//
// found=false 是正常结果，表示该节点不持有这份副本（或已删除），
// 与"节点不可达"严格区分：前者参与法定人数统计，后者不参与。
func (c *PeerClient) GetMeta(ctx context.Context, bucket, key string) (*store.ObjectMeta, bool, error) {
	var out metaResponse
	if err := c.do(ctx, http.MethodGet, pathMeta+"?"+objectQuery(bucket, key), nil, &out); err != nil {
		return nil, false, err
	}
	if out.Found && out.Meta == nil {
		return nil, false, fmt.Errorf("节点 %s: 协议错误: found=true 但 meta 为空", c.peer.ID)
	}
	return out.Meta, out.Found, nil
}

// PutMeta 向远端节点提交一份副本元数据。
//
// 版本冲突（409）会以 *PeerError 返回，调用方据此判断"对端已有更新版本"，
// 这不算节点故障。
func (c *PeerClient) PutMeta(ctx context.Context, bucket, key string, m *store.ObjectMeta) error {
	body, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("节点 %s: 编码副本元数据失败: %w", c.peer.ID, err)
	}
	return c.do(ctx, http.MethodPut, pathMeta+"?"+objectQuery(bucket, key), body, nil)
}

// DeleteMeta 删除远端节点上的副本，返回删除前是否存在。
func (c *PeerClient) DeleteMeta(ctx context.Context, bucket, key string) (bool, error) {
	var out struct {
		Found bool `json:"found"`
	}
	if err := c.do(ctx, http.MethodDelete, pathMeta+"?"+objectQuery(bucket, key), nil, &out); err != nil {
		return false, err
	}
	return out.Found, nil
}

// objectQuery 构造 bucket/key 的 query 串。
//
// 用 url.Values.Encode：它对 key 里的 `/`、`+`、`&`、非 ASCII 等一律
// 百分号编码，且排序是确定的。服务端 RawQuery 原样参与签名，
// 两边只要用同一套编码就不会出现"签了 A、发出去变成 B"。
func objectQuery(bucket, key string) string {
	return url.Values{"bucket": {bucket}, "key": {key}}.Encode()
}

// doTransfer 发送一个已签名的块传输请求，不读取响应体。
func (c *PeerClient) doTransfer(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+c.peer.Addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("节点 %s: 构造请求 %s %s 失败: %w", c.peer.ID, method, path, err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	SignRequest(req, c.secret, c.selfID, body, time.Now())

	resp, err := c.transfer.Do(req)
	if err != nil {
		return nil, fmt.Errorf("节点 %s: %s %s 失败: %w", c.peer.ID, method, path, err)
	}
	return resp, nil
}

// peerError 从非 200 响应构造 *PeerError 并关闭响应体。
// 只截取 512 字节：错误文本是给人看的，不是第二个数据通道。
func (c *PeerClient) peerError(method, path string, resp *http.Response) error {
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return &PeerError{
		Peer:    c.peer.ID,
		Method:  method,
		Path:    path,
		Status:  resp.StatusCode,
		Message: strings.TrimSpace(string(snippet)),
	}
}


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
		return c.peerError(method, path, resp)
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
