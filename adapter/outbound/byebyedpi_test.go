package outbound

import (
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestByeByeDPISocksBackendKeepsProxyIdentity(t *testing.T) {
	proxy, err := NewByeByeDPI(ByeByeDPIOption{
		Name:     "BBDPI",
		Strategy: "fixed",
		Args:     []string{"-d1", "-S", "-a1"},
		UDP:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := proxy.socksBackend("127.0.0.1:12345")
	if backend.Type() != C.ByeByeDPI {
		t.Fatalf("unexpected backend type: %s", backend.Type())
	}
	if backend.Name() != "BBDPI" {
		t.Fatalf("unexpected backend name: %s", backend.Name())
	}
	if !backend.SupportUDP() {
		t.Fatal("UDP support should be preserved")
	}
}

func TestByeByeDPIRejectsUserProtectPath(t *testing.T) {
	for _, args := range [][]string{
		{"-P", "/tmp/byedpi.sock"},
		{"-P/tmp/byedpi.sock"},
		{"--protect-path", "/tmp/byedpi.sock"},
		{"--protect-path=/tmp/byedpi.sock"},
	} {
		if _, err := NewByeByeDPI(ByeByeDPIOption{
			Name:     "BBDPI",
			Strategy: "fixed",
			Args:     args,
		}); err == nil {
			t.Fatalf("expected protect path rejection for args %v", args)
		}
	}
}
