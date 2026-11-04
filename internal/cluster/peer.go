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

// maxListResponseBytes 是内部 LIST 响应的读取上限。
//
// 比普通 JSON 的上限大得多：一个桶的清单随对象数线性增长，10 万条
// ReplicaEntry 约 20 MB。上限存在的意义是挡住畸形/恶意响应，而不是
// 限制合法规模；超过它时调用方得到明确错误，而不是把内存吃光。
const maxListResponseBytes = 64 << 20

// listTimeout 是单节点 LIST（以及其它可能较大的 JSON 响应）的超时。
//
// 不能用 PeerTimeout（2s）：那是给几十字节 JSON 用的。本机环回上
// 10 万对象的扫描 + 序列化 + 传输仍在秒级，30s 只用来把"对端僵死"
// 变成一个可统计的失败。
const listTimeout = 30 * time.Second

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
	// bulk 承载可能返回大量 JSON 的请求（集群 LIST、整桶清单）。
	//
	// 与 client 分开是为了让超时与响应上限都按量级匹配：client 的 2s
	// 会把一次正常的万级对象 LIST 误判成节点故障，进而让整个集群 LIST
	// 回 503；而把 2s 全局调大又会让"节点僵死"的探测变慢。
	bulk *http.Client
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
		bulk:   &http.Client{Timeout: listTimeout, CheckRedirect: checkRedirect},
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
	c.bulk.CloseIdleConnections()
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
//
// 注意：对象删除**不再**走这里。删除写入的是墓碑元数据（保证删除的
// 版本能被副本比较看到），物理删掉 meta 目录会让宕机节点恢复后无据可查。
// 本方法保留给切片 6 的墓碑回收：那时由确定性 owner 在确认全部副本
// 已过 TTL 之后物理清理。
func (c *PeerClient) DeleteMeta(ctx context.Context, bucket, key string) (bool, error) {
	var out struct {
		Found bool `json:"found"`
	}
	if err := c.do(ctx, http.MethodDelete, pathMeta+"?"+objectQuery(bucket, key), nil, &out); err != nil {
		return false, err
	}
	return out.Found, nil
}

// ListReplicas 取远端节点上某桶的全部副本条目（含墓碑）。
//
// 走 doBulk：整桶清单是唯一一个响应体随数据规模增长的内部接口，
// 用普通请求的 2s 超时会把一个健康但有十万对象的节点误判成故障。
func (c *PeerClient) ListReplicas(ctx context.Context, bucket string) (listResponse, error) {
	var out listResponse
	if err := c.doBulk(ctx, http.MethodGet, pathList+"?"+bucketQuery(bucket), nil, &out); err != nil {
		return listResponse{}, err
	}
	return out, nil
}

// BucketInfo 读取远端节点上的桶元数据；桶不存在时 Exists=false。
func (c *PeerClient) BucketInfo(ctx context.Context, bucket string) (bucketInfo, error) {
	var out bucketInfo
	if err := c.do(ctx, http.MethodGet, pathBucket+"?"+bucketQuery(bucket), nil, &out); err != nil {
		return bucketInfo{}, err
	}
	return out, nil
}

// BucketState 读取远端节点上桶的存在性与空否。
func (c *PeerClient) BucketState(ctx context.Context, bucket string) (bucketState, error) {
	var out bucketState
	if err := c.do(ctx, http.MethodGet, pathBucketState+"?"+bucketQuery(bucket), nil, &out); err != nil {
		return bucketState{}, err
	}
	return out, nil
}

// ListBuckets 读取远端节点的桶名列表。
func (c *PeerClient) ListBuckets(ctx context.Context) ([]string, error) {
	var out bucketsResponse
	if err := c.do(ctx, http.MethodGet, pathBuckets, nil, &out); err != nil {
		return nil, err
	}
	return out.Buckets, nil
}

// CreateBucket 在远端节点创建桶。重复创建不算错误（对端幂等）。
func (c *PeerClient) CreateBucket(ctx context.Context, bucket string) error {
	return c.do(ctx, http.MethodPut, pathBucket+"?"+bucketQuery(bucket), nil, nil)
}

// DeleteBucket 在远端节点删除桶。
func (c *PeerClient) DeleteBucket(ctx context.Context, bucket string) error {
	return c.do(ctx, http.MethodDelete, pathBucket+"?"+bucketQuery(bucket), nil, nil)
}

// GetCORS 读取远端节点上的桶 CORS 配置。
//
// found=false 表示桶在但没有配置，与"节点不可达"由 error 区分。
func (c *PeerClient) GetCORS(ctx context.Context, bucket string) (*store.CORSConfiguration, bool, error) {
	var out corsResponse
	if err := c.do(ctx, http.MethodGet, pathCORS+"?"+bucketQuery(bucket), nil, &out); err != nil {
		return nil, false, err
	}
	if out.Found && out.Config == nil {
		return nil, false, fmt.Errorf("节点 %s: 协议错误: found=true 但 CORS 配置为空", c.peer.ID)
	}
	return out.Config, out.Found, nil
}

// PutCORS 向远端节点写入桶 CORS 配置。
func (c *PeerClient) PutCORS(ctx context.Context, bucket string, cfg *store.CORSConfiguration) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("节点 %s: 编码 CORS 配置失败: %w", c.peer.ID, err)
	}
	return c.do(ctx, http.MethodPut, pathCORS+"?"+bucketQuery(bucket), body, nil)
}

// DeleteCORS 删除远端节点上的桶 CORS 配置（未配置也成功）。
func (c *PeerClient) DeleteCORS(ctx context.Context, bucket string) error {
	return c.do(ctx, http.MethodDelete, pathCORS+"?"+bucketQuery(bucket), nil, nil)
}

// bucketQuery 构造只带桶名的 query 串。
func bucketQuery(bucket string) string {
	return url.Values{"bucket": {bucket}}.Encode()
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
	return c.doWith(ctx, c.client, maxResponseBytes, method, path, body, out)
}

// doBulk 与 do 相同，但使用大响应的超时与上限。
func (c *PeerClient) doBulk(ctx context.Context, method, path string, body []byte, out any) error {
	return c.doWith(ctx, c.bulk, maxListResponseBytes, method, path, body, out)
}

// doWith 发送一个已签名的 JSON 请求并解析响应。
//
// client 与 limit 由调用方按响应量级选择：小响应快速失败，大响应
// 给足时间。签名、状态码与解析逻辑只有这一份，两种量级不会漂移。
func (c *PeerClient) doWith(ctx context.Context, client *http.Client, limit int, method, path string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+c.peer.Addr+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("节点 %s: 构造请求 %s %s 失败: %w", c.peer.ID, method, path, err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	SignRequest(req, c.secret, c.selfID, body, time.Now())

	resp, err := client.Do(req)
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

	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return fmt.Errorf("节点 %s: 读取 %s 响应失败: %w", c.peer.ID, path, err)
	}
	if len(data) > limit {
		return fmt.Errorf("节点 %s: %s 响应超过 %d 字节上限", c.peer.ID, path, limit)
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
