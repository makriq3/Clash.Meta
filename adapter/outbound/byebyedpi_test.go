package outbound

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

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

func TestByeByeDPIDialContextRelaysTCP(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("ok")); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	proxy, err := NewByeByeDPI(ByeByeDPIOption{
		Name:     "BBDPI",
		Strategy: "fixed",
		Args:     []string{"-Ku", "-a1", "-An", "-o1", "-At,r,s", "-d1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	addr := listener.Addr().(*net.TCPAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := proxy.DialContext(ctx, &C.Metadata{
		DstIP:   addr.AddrPort().Addr(),
		DstPort: uint16(addr.Port),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ok" {
		t.Fatalf("unexpected payload: %q", string(buf))
	}

	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestByeByeDPIDialContextRelaysDomainTCP(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("ok")); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	proxy, err := NewByeByeDPI(ByeByeDPIOption{
		Name:     "BBDPI",
		Strategy: "fixed",
		Args:     []string{"-Ku", "-a1", "-An", "-o1", "-At,r,s", "-d1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	addr := listener.Addr().(*net.TCPAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := proxy.DialContext(ctx, &C.Metadata{
		Host:    "127.0.0.1",
		DstPort: uint16(addr.Port),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ok" {
		t.Fatalf("unexpected payload: %q", string(buf))
	}

	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
