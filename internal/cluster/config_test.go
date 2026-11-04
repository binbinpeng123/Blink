package cluster

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// validConfig 返回一份可以通过校验的三节点配置，供各测试按需改动一项。
func validConfig() Config {
	return Config{
		NodeID: "n1",
		Peers: []Peer{
			{ID: "n1", Addr: "127.0.0.1:9101"},
			{ID: "n2", Addr: "127.0.0.1:9102"},
			{ID: "n3", Addr: "127.0.0.1:9103"},
		},
		InternalAddr: "127.0.0.1:9100",
		Secret:       bytes.Repeat([]byte("k"), MinSecretLen),
		ReplicaCount: 3,
		WriteQuorum:  2,
		ReadQuorum:   2,
		VNodes:       128,
		PeerTimeout:  time.Second,
	}
}

func TestConfigValidate(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("基线配置应通过校验: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"缺 node-id", func(c *Config) { c.NodeID = "" }, "节点 ID 不能为空"},
		{"node-id 不在成员表", func(c *Config) { c.NodeID = "n9" }, "未出现在成员表中"},
		{"密钥过短", func(c *Config) { c.Secret = []byte("short") }, "至少 32 字节"},
		{"成员表为空", func(c *Config) { c.Peers = nil }, "成员表不能为空"},
		{"重复节点 ID", func(c *Config) { c.Peers[2].ID = "n2" }, "重复节点 ID"},
		{"重复地址", func(c *Config) { c.Peers[2].Addr = c.Peers[1].Addr }, "重复地址"},
		{"副本数超成员数", func(c *Config) { c.ReplicaCount = 4 }, "replica-count"},
		{"写法定人数超副本数", func(c *Config) { c.WriteQuorum = 4 }, "write-quorum"},
		{"读法定人数为零", func(c *Config) { c.ReadQuorum = 0 }, "read-quorum"},
		{"W+R 不大于 N", func(c *Config) { c.WriteQuorum, c.ReadQuorum = 1, 2 }, "必须大于"},
		{"vnodes 为零", func(c *Config) { c.VNodes = 0 }, "vnodes"},
		{"超时非正", func(c *Config) { c.PeerTimeout = 0 }, "peer-timeout"},
		{"内部地址为空", func(c *Config) { c.InternalAddr = "" }, "内部监听地址"},
		{"内部地址非法", func(c *Config) { c.InternalAddr = "no-port" }, "内部监听地址"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("期望校验失败，实际通过")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息 %q 未包含 %q", err.Error(), tc.want)
			}
		})
	}
}

// 单节点 2+1 之类的配置也必须是合法的：EC 会用到较小的 N。
func TestConfigValidateSmallCluster(t *testing.T) {
	cfg := validConfig()
	cfg.Peers = cfg.Peers[:1]
	cfg.ReplicaCount, cfg.WriteQuorum, cfg.ReadQuorum = 1, 1, 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("单节点集群配置应通过: %v", err)
	}
}

func TestParsePeers(t *testing.T) {
	peers, err := ParsePeers(" n1@127.0.0.1:9101 , n2@[::1]:9102 ")
	if err != nil {
		t.Fatalf("合法成员表解析失败: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("解析出 %d 个成员，期望 2", len(peers))
	}
	if peers[0] != (Peer{ID: "n1", Addr: "127.0.0.1:9101"}) {
		t.Errorf("第 1 个成员 = %+v", peers[0])
	}
	if peers[1] != (Peer{ID: "n2", Addr: "[::1]:9102"}) {
		t.Errorf("第 2 个成员 = %+v（IPv6 应保留方括号）", peers[1])
	}

	bad := []struct {
		name string
		in   string
		want string
	}{
		{"空串", "", "为空"},
		{"只有空格", "   ", "为空"},
		{"缺 @", "n1:9101", "id@host:port"},
		{"缺节点 ID", "@127.0.0.1:9101", "id@host:port"},
		{"缺地址", "n1@", "id@host:port"},
		{"缺端口", "n1@127.0.0.1", "地址无效"},
		{"端口越界", "n1@127.0.0.1:70000", "端口无效"},
		{"端口非数字", "n1@127.0.0.1:abc", "端口无效"},
		{"重复 ID", "n1@127.0.0.1:1,n1@127.0.0.1:2", "重复节点 ID"},
		{"重复地址", "n1@127.0.0.1:1,n2@127.0.0.1:1", "重复地址"},
		{"多余逗号", "n1@127.0.0.1:1,", "为空"},
		{"非法 ID 字符", "n 1@127.0.0.1:1", "非法字符"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePeers(tc.in)
			if err == nil {
				t.Fatal("期望解析失败，实际通过")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误信息 %q 未包含 %q", err.Error(), tc.want)
			}
		})
	}
}

func TestConfigPeerHelpers(t *testing.T) {
	cfg := validConfig()
	if p, ok := cfg.PeerByID("n2"); !ok || p.Addr != "127.0.0.1:9102" {
		t.Errorf("PeerByID(n2) = %+v, %v", p, ok)
	}
	if _, ok := cfg.PeerByID("nope"); ok {
		t.Error("未知节点不应命中")
	}
	ids := cfg.PeerIDs()
	if strings.Join(ids, ",") != "n1,n2,n3" {
		t.Errorf("PeerIDs = %v，期望按字典序排列", ids)
	}
}
