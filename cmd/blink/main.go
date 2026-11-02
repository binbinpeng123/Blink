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
	"syscall"
	"time"

	"github.com/binbinpeng123/Blink/internal/buildinfo"
	"github.com/binbinpeng123/Blink/internal/config"
	"github.com/binbinpeng123/Blink/internal/logx"
	"github.com/binbinpeng123/Blink/internal/s3api"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

// shutdownTimeout 是优雅关闭的最长等待时间，超过后强杀连接。
const shutdownTimeout = 10 * time.Second

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

	// 凭据在 config.Validate 中已确保非空，这里的 Provider 必定含一组有效凭据。
	// 单节点版本只有一组静态凭据；多租户/外部凭据源通过替换 Provider 接入。
	provider := sigv4.NewStaticProvider(sigv4.Credentials{
		AccessKeyID: cfg.AccessKey,
		SecretKey:   cfg.SecretKey,
	})

	// 打开存储。失败即拒绝启动 —— 与凭据缺失同样处理：
	// "存储不可用但 HTTP 起来了"会让所有请求拿到 500，
	// 而运维很难从一堆 500 里看出根因是数据目录挂了。
	st, err := store.Open(store.Options{
		Root:            cfg.DataDir,
		FsyncPolicy:     cfg.FsyncPolicy,
		BlockSize:       cfg.BlockSize,
		InlineThreshold: cfg.InlineThreshold,
		VerifyOnRead:    cfg.VerifyOnRead,
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

	srv := s3api.New(s3api.Options{
		Logger:       logger,
		Region:       cfg.Region,
		Provider:     provider,
		MaxClockSkew: cfg.MaxClockSkew,
		Store:        st,
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

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", cfg.Addr, err)
	}
	logger.Info("blink_listening",
		slog.String("addr", ln.Addr().String()),
		slog.String("scheme", scheme),
	)

	// 收到 SIGINT/SIGTERM 后开始优雅关闭：停止接受新连接，
	// 等待在途请求完成（最多 shutdownTimeout）。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		var err error
		if tlsCfg != nil {
			// 证书已装入 TLSConfig，certFile/keyFile 传空即可。
			err = httpSrv.ServeTLS(ln, "", "")
		} else {
			err = httpSrv.Serve(ln)
		}
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("HTTP 服务异常退出: %w", err)
		}
		return nil
	case <-ctx.Done():
		logger.Info("blink_shutting_down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("优雅关闭失败: %w", err)
	}
	if err := <-serveErr; err != nil {
		return fmt.Errorf("HTTP 服务异常退出: %w", err)
	}
	logger.Info("blink_stopped")
	return nil
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
