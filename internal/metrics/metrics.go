// Package metrics 提供最小可用的 Prometheus 文本格式指标。
//
// 不引入 prometheus/client_golang：Blink 面向边端，依赖越少越好，
// 而本切片需要的 counter/gauge/histogram 三种类型用标准库足以实现。
// 标签基数受控由调用方保证 —— 这里不做限制，但 06 文档明确只使用
// HTTP 方法、状态码、错误码与动作这类有限集合。
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// labelSeparator 用于把标签值拼成 map key。标签值由调用方控制，
// 不会包含该字符（HTTP 方法/状态码/错误码/动作）。
const labelSeparator = "\xff"

// Registry 汇总一组指标并输出 Prometheus 文本格式。
//
// 指标必须在开始服务前创建；运行期只做 Add/Observe/Set，
// 因此读列表时不需要与创建操作竞争。
type Registry struct {
	mu         sync.Mutex
	counters   []*CounterVec
	gauges     []*GaugeVec
	histograms []*HistogramVec
}

// NewRegistry 构造空注册表。
func NewRegistry() *Registry { return &Registry{} }

// Counter 创建（或复用）一个 counter 向量。
func (r *Registry) Counter(name, help string, labelNames ...string) *CounterVec {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &CounterVec{name: name, help: help, labelNames: labelNames, values: make(map[string]*atomic.Uint64)}
	r.counters = append(r.counters, c)
	return c
}

// Gauge 创建（或复用）一个 gauge 向量。
func (r *Registry) Gauge(name, help string, labelNames ...string) *GaugeVec {
	r.mu.Lock()
	defer r.mu.Unlock()
	g := &GaugeVec{name: name, help: help, labelNames: labelNames, values: make(map[string]*atomic.Uint64)}
	r.gauges = append(r.gauges, g)
	return g
}

// Histogram 创建（或复用）一个 histogram 向量，使用默认桶。
func (r *Registry) Histogram(name, help string, labelNames ...string) *HistogramVec {
	return r.HistogramWithBuckets(name, help, DefaultBuckets, labelNames...)
}

// DefaultBuckets 覆盖从 1ms 到 10s 的请求延迟，适用于对象存储的
// 小对象访问与大对象首字节。
var DefaultBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10,
}

// HistogramWithBuckets 允许调用方提供桶边界（必须升序）。
func (r *Registry) HistogramWithBuckets(name, help string, buckets []float64, labelNames ...string) *HistogramVec {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := append([]float64(nil), buckets...)
	sort.Float64s(cp)
	h := &HistogramVec{name: name, help: help, labelNames: labelNames, buckets: cp, values: make(map[string]*histSample)}
	r.histograms = append(r.histograms, h)
	return h
}

// Write 输出全部指标。输出顺序按注册顺序，同一向量的标签组合按 key 排序，
// 保证抓取结果稳定、便于 diff。
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	counters := append([]*CounterVec(nil), r.counters...)
	gauges := append([]*GaugeVec(nil), r.gauges...)
	histograms := append([]*HistogramVec(nil), r.histograms...)
	r.mu.Unlock()

	for _, c := range counters {
		c.write(w)
	}
	for _, g := range gauges {
		g.write(w)
	}
	for _, h := range histograms {
		h.write(w)
	}
}

// CounterVec 是带固定标签名的 counter 集合。
type CounterVec struct {
	name       string
	help       string
	labelNames []string
	mu         sync.RWMutex
	values     map[string]*atomic.Uint64
}

// Inc 增加 1。
func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Add 增加 delta。标签数量不匹配时静默忽略：指标不能反过来让请求失败。
func (c *CounterVec) Add(delta uint64, labelValues ...string) {
	if len(labelValues) != len(c.labelNames) {
		return
	}
	key := strings.Join(labelValues, labelSeparator)
	c.mu.RLock()
	v := c.values[key]
	c.mu.RUnlock()
	if v == nil {
		c.mu.Lock()
		v = c.values[key]
		if v == nil {
			v = &atomic.Uint64{}
			c.values[key] = v
		}
		c.mu.Unlock()
	}
	v.Add(delta)
}

func (c *CounterVec) write(w io.Writer) {
	writeHeader(w, c.name, c.help, "counter")
	c.mu.RLock()
	keys := sortedKeys(c.values)
	type entry struct {
		key string
		val uint64
	}
	entries := make([]entry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, entry{k, c.values[k].Load()})
	}
	c.mu.RUnlock()
	for _, e := range entries {
		fmt.Fprintf(w, "%s%s %d\n", c.name, formatLabels(c.labelNames, e.key), e.val)
	}
}

// GaugeVec 是带固定标签名的 gauge 集合。
type GaugeVec struct {
	name       string
	help       string
	labelNames []string
	mu         sync.RWMutex
	values     map[string]*atomic.Uint64
}

// Set 设置数值。
func (g *GaugeVec) Set(v float64, labelValues ...string) {
	if len(labelValues) != len(g.labelNames) {
		return
	}
	key := strings.Join(labelValues, labelSeparator)
	g.mu.RLock()
	slot := g.values[key]
	g.mu.RUnlock()
	if slot == nil {
		g.mu.Lock()
		slot = g.values[key]
		if slot == nil {
			slot = &atomic.Uint64{}
			g.values[key] = slot
		}
		g.mu.Unlock()
	}
	slot.Store(math.Float64bits(v))
}

// SetBool 是 0/1 gauge 的便捷写法。
func (g *GaugeVec) SetBool(b bool, labelValues ...string) {
	if b {
		g.Set(1, labelValues...)
		return
	}
	g.Set(0, labelValues...)
}

func (g *GaugeVec) write(w io.Writer) {
	writeHeader(w, g.name, g.help, "gauge")
	g.mu.RLock()
	keys := sortedKeys(g.values)
	type entry struct {
		key string
		val float64
	}
	entries := make([]entry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, entry{k, math.Float64frombits(g.values[k].Load())})
	}
	g.mu.RUnlock()
	for _, e := range entries {
		fmt.Fprintf(w, "%s%s %s\n", g.name, formatLabels(g.labelNames, e.key), formatFloat(e.val))
	}
}

// HistogramVec 是带固定标签名的 histogram 集合。
type HistogramVec struct {
	name       string
	help       string
	labelNames []string
	buckets    []float64
	mu         sync.RWMutex
	values     map[string]*histSample
}

type histSample struct {
	mu     sync.Mutex
	counts []uint64
	sum    float64
	count  uint64
}

// Observe 记录一次观测值（秒）。
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	if len(labelValues) != len(h.labelNames) {
		return
	}
	key := strings.Join(labelValues, labelSeparator)
	h.mu.RLock()
	s := h.values[key]
	h.mu.RUnlock()
	if s == nil {
		h.mu.Lock()
		s = h.values[key]
		if s == nil {
			s = &histSample{counts: make([]uint64, len(h.buckets))}
			h.values[key] = s
		}
		h.mu.Unlock()
	}
	s.observe(v, h.buckets)
}

func (s *histSample) observe(v float64, buckets []float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, b := range buckets {
		if v <= b {
			s.counts[i]++
		}
	}
	s.sum += v
	s.count++
}

func (h *HistogramVec) write(w io.Writer) {
	writeHeader(w, h.name, h.help, "histogram")
	h.mu.RLock()
	keys := sortedKeys(h.values)
	samples := make([]*histSample, 0, len(keys))
	for _, k := range keys {
		samples = append(samples, h.values[k])
	}
	h.mu.RUnlock()

	for i, key := range keys {
		s := samples[i]
		s.mu.Lock()
		counts := append([]uint64(nil), s.counts...)
		sum := s.sum
		count := s.count
		s.mu.Unlock()

		labels := formatLabels(h.labelNames, key)
		for j, b := range h.buckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name,
				appendLabel(labels, "le", formatFloat(b)), counts[j])
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name,
			appendLabel(labels, "le", "+Inf"), count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labels, formatFloat(sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labels, count)
	}
}

func writeHeader(w io.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, escapeHelp(help))
	fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// formatLabels 把用分隔符拼接的标签值还原成 `{a="x",b="y"}`。
func formatLabels(names []string, key string) string {
	if len(names) == 0 {
		return ""
	}
	vals := strings.Split(key, labelSeparator)
	if len(vals) != len(names) {
		return ""
	}
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = n + `="` + escapeLabel(vals[i]) + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// appendLabel 为已有标签集追加一个标签（用于 histogram 的 le）。
func appendLabel(existing, name, value string) string {
	kv := name + `="` + escapeLabel(value) + `"`
	if existing == "" {
		return "{" + kv + "}"
	}
	return existing[:len(existing)-1] + "," + kv + "}"
}

func escapeLabel(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

func escapeHelp(v string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(v)
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
