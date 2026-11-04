package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// Health 是节点间健康检查的响应。
//
// 字段刻意保持少量且稳定：它是滚动升级期间新旧版本唯一必须互相理解的
// 内部结构，扩字段可以，改语义不行。
type Health struct {
	NodeID    string   `json:"node_id"`
	PeerCount int      `json:"peer_count"`
	RingNodes []string `json:"ring_nodes"`
}

// Server 是节点间内部协议的 HTTP 服务。
//
// 它与 S3 端口分离监听：S3 端口面向客户端并可能暴露到公网，
// 内部端口只承载节点间请求，可以单独用 NetworkPolicy 隔离。
//
// Server 不持有集群协调逻辑：它只把本地存储的副本引擎能力暴露成
// 内部接口。写哪一个副本、凑多少 ack，是 Cluster 的事。
type Server struct {
	cfg     Config
	store   *store.Store
	logger  *slog.Logger
	ring    *Ring
	allowed map[string]struct{}
	// now 允许测试注入时钟；生产环境为 time.Now。
	now func() time.Time
}

// NewServer 构造内部服务。配置不合法时返回错误，绝不带着半配置启动。
//
// st 是本节点的单机存储：内部接口的所有数据操作都落在它上面，
// 集群层不另造一套存储实现。
func NewServer(cfg Config, st *store.Store, logger *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if st == nil {
		return nil, errors.New("cluster: 内部服务必须绑定本地存储")
	}
	if logger == nil {
		logger = slog.Default()
	}

	ids := cfg.PeerIDs()
	ring, err := NewRing(ids, cfg.VNodes)
	if err != nil {
		return nil, err
	}

	allowed := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		allowed[id] = struct{}{}
	}

	return &Server{
		cfg:     cfg,
		store:   st,
		logger:  logger,
		ring:    ring,
		allowed: allowed,
		now:     time.Now,
	}, nil
}

// Ring 返回本节点持有的哈希环（只读，并发安全）。
func (s *Server) Ring() *Ring {
	return s.ring
}

// Handler 返回装好认证中间件的 http.Handler。
//
// 认证覆盖全部内部路径（包括 404 兜底），不存在"免签的白名单"：
// 内部端口不承接任何客户端流量，没有例外可开。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/health", s.handleHealth)
	mux.HandleFunc(pathBlock, s.handleBlock)
	mux.HandleFunc(pathMeta, s.handleMeta)
	mux.HandleFunc("/", s.handleUnknown)
	return s.withAuth(mux)
}

// handleHealth 返回节点身份与成员视图，供 PeerClient 探活与排障。
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
		return
	}
	writeJSON(w, http.StatusOK, Health{
		NodeID:    s.cfg.NodeID,
		PeerCount: len(s.cfg.Peers),
		RingNodes: s.ring.Nodes(),
	})
}

// handleUnknown 对未实现的内部接口明确回 404。
//
// 不回 501：内部协议只有本项目的节点会调用，路径写错就是写错，
// 404 能让调用方立刻定位到版本不匹配，而不是去猜"是不是功能没做"。
func (s *Server) handleUnknown(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, errorBody{Error: "not_found"})
}

// withAuth 校验每个内部请求的 HMAC 与时间窗口。
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node, body, err := VerifyRequest(r, s.cfg.Secret, s.isMember, s.now())
		if err != nil {
			status := http.StatusUnauthorized
			code := "unauthenticated"
			if errors.Is(err, ErrBodyTooLarge) {
				status = http.StatusRequestEntityTooLarge
				code = "request_too_large"
			}
			// 失败原因写入日志，但不回给调用方：内部端口虽然没有外部客户端，
			// 也没必要把"时间戳超窗"还是"签名不对"告诉一个未认证方。
			s.logger.Warn("internal_auth_failed",
				slog.String("remote", r.RemoteAddr),
				slog.String("path", r.URL.EscapedPath()),
				slog.String("reason", err.Error()),
			)
			writeJSON(w, status, errorBody{Error: code, Message: "内部请求认证失败"})
			return
		}

		// 把已校验的字节交回业务处理器：签名校验读过的内容必须就是
		// 业务看到的内容，否则两者的差异就是一处可被利用的缝隙。
		r.Body = io.NopCloser(bytes.NewReader(body))
		r = r.WithContext(context.WithValue(r.Context(), nodeIDKey{}, node))
		w.Header().Set(HeaderNode, s.cfg.NodeID)
		next.ServeHTTP(w, r)
	})
}

// isMember 判断节点是否在成员表中。
func (s *Server) isMember(id string) bool {
	_, ok := s.allowed[id]
	return ok
}

// errorBody 是内部接口的错误响应。
type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// writeJSON 写出一个 JSON 响应。
//
// 先 Marshal 再写头：编码失败时还能返回 500，而不是发出一个
// 头已经写成 200、body 却是半截的响应。
func writeJSON(w http.ResponseWriter, status int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"error":"internal_error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}

// nodeIDKey 是认证后的节点 ID 在 context 中的键。
type nodeIDKey struct{}

// NodeIDFromContext 返回认证通过的调用方节点 ID；未经认证的请求为空串。
func NodeIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(nodeIDKey{}).(string)
	return id
}
