// Command blink 是 Blink S3 兼容对象存储的入口。
//
// 该二进制面向边缘设备：单文件、无外部依赖、无配置文件也能启动
// （凭据除外 —— 凭据没有默认值，缺失则拒绝启动）。
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/binbinpeng123/Blink/internal/authz"
	"github.com/binbinpeng123/Blink/internal/buildinfo"
	"github.com/binbinpeng123/Blink/internal/cluster"
	"github.com/binbinpeng123/Blink/internal/config"
	"github.com/binbinpeng123/Blink/internal/logx"
	"github.com/binbinpeng123/Blink/internal/metrics"
	"github.com/binbinpeng123/Blink/internal/s3api"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

// shutdownTimeout 是优雅关闭的最长等待时间，超过后强杀连接。
const shutdownTimeout = 10 * time.Second

// serving 描述一个监听端点：S3 端口与（集群模式下的）内部端口
// 共享同一套启动与优雅关闭流程。
type serving struct {
	name string
	srv  *http.Server
	ln   net.Listener
	tls  bool
}

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		// run 内部已打印详细信息（如参数用法），此处只补一行结果，避免刷屏。
		fmt.Fprintf(os.Stderr, "blink: 启动失败: %v\n", err)
		os.Exit(1)
	}
}

// run 是可测试的启动入口：解析配置 → 建日志 → 起服务 → 等待关闭信号。
func run(args []string, stderr io.Writer) error {
	cfg, err := config.Load(args, stderr)
	if err != nil {
		return err
	}

	logger := logx.New(cfg.LogLevel, stderr)
	logger.Info("blink_starting",
		slog.String("version", buildinfo.Version),
		slog.String("commit", buildinfo.Commit),
		slog.String("go", buildinfo.GoVersion()),
	)

	// 输出脱敏后的生效配置，便于排障；凭据字段已替换为占位符。
	attrs := make([]any, 0, len(cfg.Redacted()))
	for k, v := range cfg.Redacted() {
		attrs = append(attrs, slog.String(k, v))
	}
	logger.Info("blink_config", attrs...)

	// 单机模式不读取集群参数，但残留变量的笔误仍要可见：
	// 静默吞掉会让切到集群模式时才发现配置从未生效。
	for _, warning := range cfg.ClusterEnvWarnings() {
		logger.Warn("cluster_env_ignored", slog.String("detail", warning))
	}

	// TLS 证书在监听之前加载：证书与私钥不匹配时必须拒绝启动，
	// 而不是先占住端口、在每次握手时才失败 —— 后者会让探活"看起来正常"。
	var tlsCfg *tls.Config
	scheme := "http"
	if cfg.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			// tls 的错误信息只含路径与解析原因，不含私钥内容。
			return fmt.Errorf("加载 TLS 证书失败: %w", err)
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
			// 1.2 是 aws-cli / 主流 SDK 的共同下限；1.0、1.1 已不被接受。
			MinVersion: tls.VersionTLS12,
		}
		scheme = "https"
		logTLSInfo(logger, cert)
	}

	// 凭据在 config.Validate 中已确保非空，这里的 Provider 必定含至少一组凭据。
	//
	// 两类凭据的权限语义刻意不同（06 文档）：
	//   - 环境变量/命令行凭据：全权限，保证既有部署升级后行为不变；
	//   - 文件凭据：各自带显式规则，构造时已校验，缺规则即启动失败。
	// 策略表与 Provider 必须由同一份配置生成，否则会出现"签名能过、
	// 策略查不到"的凭据 —— 那种凭据在请求期会被拒绝，问题却暴露得很晚。
	creds := make([]sigv4.Credentials, 0, cfg.CredentialCount())
	policies := make(map[string]*authz.Policy, cfg.CredentialCount())
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		creds = append(creds, sigv4.Credentials{
			AccessKeyID: cfg.AccessKey,
			SecretKey:   cfg.SecretKey,
		})
		policies[cfg.AccessKey] = authz.AllowAll()
	}
	for _, fc := range cfg.FileCredentials {
		creds = append(creds, sigv4.Credentials{
			AccessKeyID: fc.AccessKeyID,
			SecretKey:   fc.SecretKey,
		})
		policies[fc.AccessKeyID] = fc.Policy
	}
	provider := sigv4.NewStaticProvider(creds...)

	logger.Info("credentials_loaded",
		slog.Int("total", len(policies)),
		slog.Int("from_file", len(cfg.FileCredentials)),
	)
	if cfg.AccessKey != "" {
		// 全权限凭据存在这一事实必须出现在启动日志里：它是部署审计中
		// 最需要被注意到的一条，而 Redacted() 只显示"已设置"。
		logger.Warn("env_credential_full_access",
			slog.String("access_key", logx.RedactKeyID(cfg.AccessKey)),
			slog.String("detail", "环境变量凭据拥有全部权限；生产环境建议改用 --credentials-file"),
		)
	}

	// 打开存储。失败即拒绝启动 —— 与凭据缺失同样处理：
	// "存储不可用但 HTTP 起来了"会让所有请求拿到 500，
	// 而运维很难从一堆 500 里看出根因是数据目录挂了。
	st, err := store.Open(store.Options{
		Root:            cfg.DataDir,
		FsyncPolicy:     cfg.FsyncPolicy,
		BlockSize:       cfg.BlockSize,
		InlineThreshold: cfg.InlineThreshold,
		VerifyOnRead:    cfg.VerifyOnRead,
		HighWatermark:   cfg.HighWatermark,
		LowWatermark:    cfg.LowWatermark,
	})
	if err != nil {
		return fmt.Errorf("打开存储失败: %w", err)
	}
	// 块大小与内联阈值影响的是**已落盘数据的布局**，因此必须出现在启动日志里：
	// 事后排查"这个对象为什么是 3 个块"时，日志是唯一能说明当时参数的证据。
	logger.Info("store_opened",
		slog.String("data_dir", st.Root()),
		slog.String("fsync_policy", cfg.FsyncPolicy),
		slog.Int64("block_size", cfg.BlockSize),
		slog.Int64("inline_threshold", cfg.InlineThreshold),
		// 校验读会影响读吞吐，属于"事后必须能从日志复原"的部署参数。
		slog.Bool("verify_read", cfg.VerifyOnRead),
	)

	// 存储后端按模式二选一：
	//   single  → *store.Store，调用路径与集群功能引入前逐字节等价；
	//   cluster → *cluster.Cluster，把对象读写扇出到环上的副本节点。
	// 两者都满足 store.Backend，s3api 不需要知道自己在哪种模式下运行。
	var backend store.Backend = st
	var internalHandler http.Handler
	var cl *cluster.Cluster
	if cfg.ClusterMode == config.ClusterModeCluster {
		cc, err := cfg.ClusterConfig()
		if err != nil {
			return fmt.Errorf("构造集群配置失败: %w", err)
		}
		cl, err = cluster.New(cc, st, logger)
		if err != nil {
			return fmt.Errorf("构造集群后端失败: %w", err)
		}
		defer cl.Close()
		backend = cl
		internalHandler = cl.InternalHandler()
	}

	// 容量信息供 /readyz 与 /metrics 使用。集群后端内嵌 *store.Store，
	// 类型断言对两种模式都成立；将来若出现不提供容量的后端，
	// /readyz 会如实返回 disk.available=false，而不是编造数值。
	capacity, _ := backend.(s3api.Capacity)

	srv := s3api.New(s3api.Options{
		Logger:          logger,
		Region:          cfg.Region,
		Provider:        provider,
		MaxClockSkew:    cfg.MaxClockSkew,
		Store:           backend,
		Policies:        policies,
		Metrics:         metrics.NewRegistry(),
		Capacity:        capacity,
		HighWatermark:   cfg.HighWatermark,
		LowWatermark:    cfg.LowWatermark,
		CredentialCount: len(policies),
	})

	httpSrv := &http.Server{
		Handler: srv.Handler(),
		// 不设 WriteTimeout：大对象下载可能持续很久，写超时会中途掐断传输。
		// 超时保护由 ReadHeaderTimeout 与 IdleTimeout 承担。
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          newServeErrorLog(logger),
	}
	if tlsCfg != nil {
		httpSrv.TLSConfig = tlsCfg
	}

	// serving 描述一个监听端点：S3 端口与（集群模式下）内部端口
	// 共享同一套启动与优雅关闭流程。
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", cfg.Addr, err)
	}
	logger.Info("blink_listening",
		slog.String("addr", ln.Addr().String()),
		slog.String("scheme", scheme),
	)
	// /metrics 免签名是刻意的取舍：Prometheus 抓取器通常不带 SigV4。
	// 代价必须让运维在启动日志里就看到，而不是等到日志评审时才发现。
	logger.Warn("metrics_endpoint_anonymous",
		slog.String("path", "/metrics"),
		slog.String("detail", "指标端点免签名，请用 NetworkPolicy 或反向代理限制来源"),
	)

	servers := []serving{{name: "s3", srv: httpSrv, ln: ln, tls: tlsCfg != nil}}

	// 内部端口只在集群模式监听。单机模式不打开 9100、不建环，
	// 也不产生任何与集群相关的 goroutine —— 这是"零回归"的直接保证。
	if internalHandler != nil {
		internal, err := startClusterInternal(cfg, internalHandler, logger)
		if err != nil {
			_ = ln.Close()
			return err
		}
		servers = append(servers, internal)
	}

	// 收到 SIGINT/SIGTERM 后开始优雅关闭：停止接受新连接，
	// 等待在途请求完成（最多 shutdownTimeout）。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 完整性巡检与 HTTP 服务共享生命周期 context：退出时取消正在进行的
	// 一轮，避免全盘扫描把 Pod 终止拖过 kubelet 宽限期。
	// 巡检始终针对本节点的本地存储（集群模式下也只看本地副本）。
	go runIntegrityScrubber(ctx, st, logger, cfg.IntegrityInterval)

	serveErr := make(chan error, len(servers))
	for _, s := range servers {
		go func(s serving) {
			var err error
			if s.tls {
				// 证书已装入 TLSConfig，certFile/keyFile 传空即可。
				err = s.srv.ServeTLS(s.ln, "", "")
			} else {
				err = s.srv.Serve(s.ln)
			}
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			if err != nil {
				err = fmt.Errorf("%s 服务异常退出: %w", s.name, err)
			}
			serveErr <- err
		}(s)
	}

	select {
	case err := <-serveErr:
		if err != nil {
			// 一个端点异常退出时，把其余的也一并关掉：
			// 带着半套服务继续跑，比直接退出更难排查。
			closeCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			for _, s := range servers {
				_ = s.srv.Shutdown(closeCtx)
			}
			cancel()
			return err
		}
		return nil
	case <-ctx.Done():
		logger.Info("blink_shutting_down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// 并行关闭：多个端点的等待时间不应叠加，否则滚动升级时
	// 一个慢连接就能把整个进程的退出拖过 kubelet 的宽限期。
	var wg sync.WaitGroup
	shutdownErr := make(chan error, len(servers))
	for _, s := range servers {
		wg.Add(1)
		go func(s serving) {
			defer wg.Done()
			if err := s.srv.Shutdown(shutdownCtx); err != nil {
				shutdownErr <- fmt.Errorf("%s 优雅关闭失败: %w", s.name, err)
			}
		}(s)
	}
	wg.Wait()
	close(shutdownErr)
	for err := range shutdownErr {
		return err
	}
	for range servers {
		if err := <-serveErr; err != nil {
			return err
		}
	}
	logger.Info("blink_stopped")
	return nil
}

// runIntegrityScrubber 按固定周期在后台巡检本节点已落盘的数据。
//
// 与读取时校验（--verify-read）互补：后者只在对象被访问时才发现损坏，
// 长期不被读取的冷数据可能坏了很久都没人知道。巡检主动把每个对象读
// 一遍并重算 CRC32C，把"静默损坏"变成可告警的指标与日志。
//
// 第一轮刻意**不**在启动时立即执行：边端 Pod 滚动重启频繁，立即扫描
// 会让每次重启都触发一轮全盘读，把启动后的前台读延迟打满。第一轮在
// 第一个周期之后才开始（默认 24h）。
//
// interval <= 0 表示禁用；单机与集群模式都生效。集群模式下每个节点
// 只巡检本地已有的数据，分片缺失属于反熵修复的职责，不算损坏。
func runIntegrityScrubber(ctx context.Context, st *store.Store, logger *slog.Logger, interval time.Duration) {
	if st == nil || interval <= 0 {
		logger.Info("scrub_disabled", slog.String("reason", "integrity_interval <= 0"))
		return
	}
	logger.Info("scrub_scheduled",
		slog.Duration("interval", interval),
		// 把"首轮延迟"明确写进日志，避免运维误以为启动即巡检。
		slog.Duration("first_run_after", interval),
	)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		stats, err := st.ScrubAll(ctx)
		if err != nil {
			// 关闭时的取消不是故障，直接退出，不再刷一条误导性的错误日志。
			if ctx.Err() != nil {
				return
			}
			logger.Error("scrub_failed", slog.Any("err", err))
			continue
		}
		if stats.CorruptObjects > 0 {
			// 明细已被 Store 限制在前 100 条；计数不受限，二者一起给出。
			for _, c := range stats.Corruptions {
				logger.Warn("scrub_corrupt",
					slog.String("bucket", c.Bucket),
					slog.String("key", c.Key),
					slog.String("err", c.Err),
				)
			}
			logger.Error("scrub_completed",
				slog.Int("buckets", stats.Buckets),
				slog.Int("scanned_objects", stats.ScannedObjects),
				slog.Int("corrupt_objects", stats.CorruptObjects),
				slog.Int("skipped_objects", stats.SkippedObjects),
				slog.Duration("duration", stats.Duration),
			)
			continue
		}
		logger.Info("scrub_completed",
			slog.Int("buckets", stats.Buckets),
			slog.Int("scanned_objects", stats.ScannedObjects),
			slog.Int("corrupt_objects", 0),
			slog.Int("skipped_objects", stats.SkippedObjects),
			slog.Duration("duration", stats.Duration),
		)
	}
}

// startClusterInternal 构造并监听节点间内部协议端口。
//
// 内部端口刻意使用明文 HTTP：它在可信内网中由 NetworkPolicy 隔离，
// 请求本身有 HMAC 认证；为它再配一套证书会把边端部署的运维成本
// 推高到与收益不相称的程度（08 文档）。
func startClusterInternal(cfg config.Config, handler http.Handler, logger *slog.Logger) (serving, error) {
	cc, err := cfg.ClusterConfig()
	if err != nil {
		return serving{}, fmt.Errorf("构造集群配置失败: %w", err)
	}

	ln, err := net.Listen("tcp", cc.InternalAddr)
	if err != nil {
		return serving{}, fmt.Errorf("监听内部端口 %s 失败: %w", cc.InternalAddr, err)
	}
	logger.Info("cluster_internal_listening",
		slog.String("node_id", cc.NodeID),
		slog.String("addr", ln.Addr().String()),
		slog.Int("peers", len(cc.Peers)),
		slog.Int("replicas", cc.ReplicaCount),
		slog.Int("write_quorum", cc.WriteQuorum),
		slog.Int("read_quorum", cc.ReadQuorum),
		slog.Int("vnodes", cc.VNodes),
	)

	return serving{
		name: "internal",
		srv: &http.Server{
			Handler: handler,
			// 内部请求体较小（≤ block-size），头部超时足以挡住慢速攻击。
			// 不设 WriteTimeout：后续切片会有流式块传输。
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       60 * time.Second,
			ErrorLog:          newServeErrorLog(logger),
		},
		ln:  ln,
		tls: false,
	}, nil
}

// logTLSInfo 输出证书的公开信息（主题、SAN、到期时间）。
//
// 这些字段全部来自证书本身，属于公开信息；私钥内容不在证书里，
// 也绝不会被写入日志。到期预警是为了让"证书过期导致全客户端失败"
// 在启动日志里就有迹可循。
func logTLSInfo(logger *slog.Logger, cert tls.Certificate) {
	leaf := cert.Leaf
	if leaf == nil && len(cert.Certificate) > 0 {
		if parsed, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
			leaf = parsed
		}
	}
	if leaf == nil {
		logger.Info("tls_enabled")
		return
	}
	logger.Info("tls_enabled",
		slog.String("cert_subject", leaf.Subject.String()),
		slog.String("cert_dns_names", strings.Join(leaf.DNSNames, ",")),
		slog.Time("cert_not_after", leaf.NotAfter),
	)
	if time.Now().After(leaf.NotAfter) {
		logger.Warn("tls_cert_expired", slog.Time("cert_not_after", leaf.NotAfter))
	}
}

// slogWriter 把 net/http 内部的标准库日志转成结构化日志，
// 使访问日志与服务器错误日志出现在同一路输出中。
type slogWriter struct{ logger *slog.Logger }

// Write 实现 io.Writer；http.Server 只会按行写入，无需额外缓冲。
func (w slogWriter) Write(p []byte) (int, error) {
	w.logger.Warn("http_server", slog.String("msg", string(trimNewline(p))))
	return len(p), nil
}

// trimNewline 去掉日志行尾的换行，保持 JSON 输出整洁。
func trimNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// newServeErrorLog 构造 http.Server.ErrorLog 所需的 *log.Logger。
func newServeErrorLog(logger *slog.Logger) *log.Logger {
	return log.New(slogWriter{logger: logger}, "", 0)
}
