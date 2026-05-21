package config_test

import (
	"testing"

	"github.com/metacubex/mihomo/config"
	C "github.com/metacubex/mihomo/constant"
	_ "github.com/metacubex/mihomo/hub/executor"
)

func TestParseByeByeDPIProxyInConfigAndGroup(t *testing.T) {
	cfg, err := config.Parse([]byte(`
proxies:
  - name: BBDPI
    type: byebyedpi
    strategy: auto
    udp: true
    auto:
      mode: both
    strategies:
      - name: default
        args:
          - "-o1"
          - "-At,r,s"
proxy-groups:
  - name: Auto
    type: select
    proxies:
      - BBDPI
      - DIRECT
rules:
  - MATCH,Auto
`))
	if err != nil {
		t.Fatal(err)
	}
	proxy, ok := cfg.Proxies["BBDPI"]
	if !ok {
		t.Fatal("BBDPI proxy was not parsed")
	}
	if proxy.Type() != C.ByeByeDPI {
		t.Fatalf("unexpected proxy type: %s", proxy.Type())
	}
	if _, ok := cfg.Proxies["Auto"]; !ok {
		t.Fatal("proxy group was not parsed")
	}
}
