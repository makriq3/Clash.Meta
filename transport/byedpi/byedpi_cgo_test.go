//go:build cgo && linux

package byedpi

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/metacubex/mihomo/constant"
)

func TestStartUsesUnixSocketBackend(t *testing.T) {
	withTempHome(t)

	inst, err := Start([]string{"-Ku", "-a1", "-An", "-d1"})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	if inst.Network() != "unix" {
		t.Fatalf("unexpected backend network: %q", inst.Network())
	}
	if inst.Addr() == "" {
		t.Fatal("backend socket path is empty")
	}
	if _, err := os.Stat(inst.Addr()); err != nil {
		t.Fatalf("backend socket was not created: %v", err)
	}
	if _, err := net.DialTimeout("tcp4", inst.Addr(), 100*time.Millisecond); err == nil {
		t.Fatal("backend address must not be reachable as TCP")
	}
}

func TestStartReusesSameBackendAndRestartsOnArgsChange(t *testing.T) {
	withTempHome(t)

	first, err := Start([]string{"-Ku", "-a1", "-An", "-d1"})
	if err != nil {
		t.Fatal(err)
	}
	firstAddr := first.Addr()

	second, err := Start([]string{"-Ku", "-a1", "-An", "-d1"})
	if err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	if second.Addr() != firstAddr {
		_ = first.Close()
		_ = second.Close()
		t.Fatalf("same args should reuse backend socket: %q != %q", second.Addr(), firstAddr)
	}
	if err := first.Close(); err != nil {
		_ = second.Close()
		t.Fatal(err)
	}
	if _, err := os.Stat(firstAddr); err != nil {
		_ = second.Close()
		t.Fatalf("backend socket disappeared while another reference is active: %v", err)
	}

	third, err := Start([]string{"-Ku", "-a1", "-An", "-d2"})
	if err != nil {
		_ = second.Close()
		t.Fatal(err)
	}
	defer third.Close()
	if third.Addr() == firstAddr {
		_ = second.Close()
		t.Fatal("different args should restart backend on a different socket")
	}
	if _, err := net.DialTimeout("unix", firstAddr, 100*time.Millisecond); err == nil {
		_ = second.Close()
		t.Fatal("old backend socket is still accepting after args change")
	}
	_ = second.Close()
}

func TestCloseStopsBackendAndRemovesSocket(t *testing.T) {
	withTempHome(t)

	inst, err := Start([]string{"-Ku", "-a1", "-An", "-d1"})
	if err != nil {
		t.Fatal(err)
	}
	addr := inst.Addr()
	if err := inst.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(addr); !os.IsNotExist(err) {
		t.Fatalf("backend socket should be removed after Close, stat err=%v", err)
	}
}

func TestStartFailureRemovesSocket(t *testing.T) {
	withTempHome(t)

	args := []string{"--definitely-invalid-byedpi-option"}
	socketPath, err := buildSocketPath("", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Start(args); err == nil {
		t.Fatal("expected invalid args to fail backend startup")
	}
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("backend socket should be removed after failed Start, stat err=%v", err)
	}
}

func TestBuildSocketPathFallsBackWhenHomeDirIsEmpty(t *testing.T) {
	oldHome := constant.Path.HomeDir()
	constant.SetHomeDir("")
	t.Cleanup(func() {
		constant.SetHomeDir(oldHome)
	})

	socketPath, err := buildSocketPath("", []string{"-d1"})
	if err != nil {
		t.Fatal(err)
	}
	if socketPath == "" {
		t.Fatal("socket path is empty")
	}
}

func withTempHome(t *testing.T) {
	t.Helper()
	home, err := os.MkdirTemp(os.TempDir(), "bd-")
	if err != nil {
		t.Fatal(err)
	}
	oldHome := constant.Path.HomeDir()
	constant.SetHomeDir(home)
	t.Cleanup(func() {
		constant.SetHomeDir(oldHome)
		_ = os.RemoveAll(home)
	})
}
