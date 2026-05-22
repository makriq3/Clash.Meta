package adapter

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestParseByeByeDPIProxyDefaults(t *testing.T) {
	proxy, err := ParseProxy(map[string]any{
		"name": "BBDPI",
		"type": "byebyedpi",
	})
	if err != nil {
		t.Fatal(err)
	}
	if proxy.Name() != "BBDPI" {
		t.Fatalf("unexpected proxy name: %s", proxy.Name())
	}
	if proxy.Type() != C.ByeByeDPI {
		t.Fatalf("unexpected proxy type: %s", proxy.Type())
	}
	if proxy.SupportUDP() {
		t.Fatal("UDP should be disabled by default")
	}
}

func TestParseByeByeDPIProxyFixedArgs(t *testing.T) {
	proxy, err := ParseProxy(map[string]any{
		"name":     "BBDPI",
		"type":     "byebyedpi",
		"strategy": "fixed",
		"args":     []any{"-Ku", "-a1", "-An", "-s1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if proxy.SupportUDP() {
		t.Fatal("UDP should stay disabled")
	}
}

func TestParseByeByeDPIProxyRejectsUDP(t *testing.T) {
	_, err := ParseProxy(map[string]any{
		"name": "BBDPI",
		"type": "byebyedpi",
		"udp":  true,
	})
	if err == nil {
		t.Fatal("expected udp=true to be rejected")
	}
}

func TestParseByeByeDPIProxyBenchmarkUsesDefaultStrategies(t *testing.T) {
	proxy, err := ParseProxy(map[string]any{
		"name":     "BBDPI",
		"type":     "byebyedpi",
		"strategy": "benchmark",
	})
	if err != nil {
		t.Fatal(err)
	}
	if proxy.Type() != C.ByeByeDPI {
		t.Fatalf("unexpected proxy type: %s", proxy.Type())
	}
}

func TestParseByeByeDPIProxyRejectsListenerArgs(t *testing.T) {
	for _, arg := range []string{"-i127.0.0.1", "--ip=127.0.0.1", "-p1080", "--port=1080", "-z/tmp/byedpi.sock", "--unix-socket=/tmp/byedpi.sock"} {
		_, err := ParseProxy(map[string]any{
			"name": "BBDPI",
			"type": "byebyedpi",
			"args": []any{arg},
		})
		if err == nil {
			t.Fatalf("expected listener arg %q to be rejected", arg)
		}
	}
}
