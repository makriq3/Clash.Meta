//go:build cgo && (linux || android)

package byedpi

/*
#cgo CFLAGS: -D_GNU_SOURCE -DSTR_MODE -std=c99 -O2 -Wall -Wno-unused -Wno-unused-parameter -Wno-missing-field-initializers
#cgo android CFLAGS: -DANDROID_APP
#cgo android LDFLAGS: -llog
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "params.h"

extern int server_fd;
int byedpi_main(int argc, char **argv);
void stop_event_loop(void);
void clear_params(char *line, char **argv);

static struct params default_params_go = {
	.await_int = 10,
	.ipv6 = 1,
	.resolve = 1,
	.udp = 1,
	.max_open = 512,
	.bfsize = 16384,
	.baddr = {
		.in6 = { .sin6_family = AF_INET6 }
	},
	.laddr = {
		.in = { .sin_family = AF_INET }
	},
	.debug = 0
};

static void byedpi_reset_params(void) {
	clear_params(NULL, NULL);
	params = default_params_go;
	optind = 1;
	server_fd = -1;
}

static int byedpi_start(int argc, char **argv) {
	byedpi_reset_params();
	return byedpi_main(argc, argv);
}

static void byedpi_stop(void) {
	if (server_fd >= 0) {
		stop_event_loop();
	}
}
*/
import "C"

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
)

var (
	runMu   sync.Mutex
	current *sharedBackend
)

type Instance struct {
	backend *sharedBackend
}

type sharedBackend struct {
	key        string
	network    string
	addr       string
	socketPath string
	done       chan int
	refs       int
	stopOnce   sync.Once
	stopped    atomic.Bool
	exitCode   atomic.Int32
}

func Start(args []string) (*Instance, error) {
	protectPath := os.Getenv("BBDPI_PROTECT_PATH")
	if protectPath == "" && runtime.GOOS == "android" {
		return nil, errors.New("byedpi protect path is not configured")
	}
	socketPath, err := buildSocketPath(protectPath, args)
	if err != nil {
		return nil, err
	}
	key := buildBackendKey(protectPath, socketPath, args)

	runMu.Lock()
	for {
		if current != nil && current.stopped.Load() {
			current.cleanup()
			current = nil
		}
		if current == nil {
			break
		}
		if current.key == key {
			current.refs++
			inst := &Instance{backend: current}
			runMu.Unlock()
			return inst, nil
		}

		previous := current
		current = nil
		runMu.Unlock()
		previous.stop()
		_ = previous.waitStopped(2 * time.Second)
		if !previous.stopped.Load() {
			runMu.Lock()
			current = previous
			runMu.Unlock()
			return nil, errors.New("previous byedpi backend did not stop")
		}
		previous.cleanup()
		runMu.Lock()
	}

	// The embedded SOCKS backend is intentionally bound to an app-private Unix
	// socket so Android apps cannot discover or abuse a localhost TCP listener.
	_ = os.Remove(socketPath)
	fullArgs := []string{"ciadpi", "--unix-socket", socketPath}
	if protectPath != "" {
		fullArgs = append(fullArgs, "--protect-path", protectPath)
	}
	fullArgs = append(fullArgs, args...)
	log.Infoln("[ByeByeDPI] starting native backend at unix://%s, protect=%v, args=%d", socketPath, protectPath != "", len(args))

	argc := C.int(len(fullArgs))
	argv := make([]*C.char, len(fullArgs))
	for i, arg := range fullArgs {
		argv[i] = C.CString(arg)
	}
	argv = append(argv, nil)
	done := make(chan int, 1)
	backend := &sharedBackend{
		key:        key,
		network:    "unix",
		addr:       socketPath,
		socketPath: socketPath,
		done:       done,
		refs:       1,
	}
	current = backend
	runMu.Unlock()
	go func() {
		code := int(C.byedpi_start(argc, (**C.char)(unsafe.Pointer(&argv[0]))))
		for _, arg := range argv[:len(argv)-1] {
			C.free(unsafe.Pointer(arg))
		}
		if code != 0 {
			log.Warnln("[ByeByeDPI] native backend exited with code %d", code)
		} else {
			log.Infoln("[ByeByeDPI] native backend stopped")
		}
		backend.exitCode.Store(int32(code))
		backend.stopped.Store(true)
		done <- code
		close(done)
	}()
	inst := &Instance{backend: backend}
	if err := inst.waitReady(2 * time.Second); err != nil {
		backend.stop()
		_ = backend.waitStopped(2 * time.Second)
		backend.cleanup()
		runMu.Lock()
		if current == backend {
			current = nil
		}
		runMu.Unlock()
		log.Warnln("[ByeByeDPI] native backend did not become ready: %v", err)
		return nil, err
	}
	log.Infoln("[ByeByeDPI] native backend ready at unix://%s", backend.addr)
	return inst, nil
}

func (i *Instance) Network() string {
	if i == nil || i.backend == nil {
		return ""
	}
	return i.backend.network
}

func (i *Instance) Addr() string {
	if i == nil || i.backend == nil {
		return ""
	}
	return i.backend.addr
}

func (i *Instance) Close() error {
	if i == nil || i.backend == nil {
		return nil
	}
	runMu.Lock()
	if i.backend.refs > 0 {
		i.backend.refs--
	}
	backend := i.backend
	shouldStop := backend.refs == 0 && current == backend
	if shouldStop {
		current = nil
	}
	runMu.Unlock()
	i.backend = nil
	if shouldStop {
		backend.stop()
		if !backend.waitStopped(2 * time.Second) {
			return errors.New("byedpi backend did not stop")
		}
		backend.cleanup()
	}
	return nil
}

func buildSocketPath(protectPath string, args []string) (string, error) {
	homeDir := constant.Path.HomeDir()
	if homeDir == "" {
		homeDir = os.TempDir()
	}
	dir := filepath.Join(homeDir, "run")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(buildConfigKey(protectPath, args)))
	name := "byedpi-" + hex.EncodeToString(sum[:8]) + ".sock"
	path := filepath.Join(dir, name)
	const unixSocketPathLimit = 100
	if len(path) >= unixSocketPathLimit {
		return "", fmt.Errorf("byedpi unix socket path is too long: length=%d limit=%d path=%q", len(path), unixSocketPathLimit-1, path)
	}
	return path, nil
}

func (i *Instance) waitReady(timeout time.Duration) error {
	if i == nil || i.backend == nil {
		return errors.New("byedpi backend is not initialized")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout(i.backend.network, i.backend.addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		select {
		case code := <-i.backend.done:
			return errors.New("byedpi backend exited with code " + strconv.Itoa(code))
		case <-time.After(50 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("byedpi backend did not start")
}

func buildConfigKey(protectPath string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, protectPath)
	parts = append(parts, args...)
	return strings.Join(parts, "\x00")
}

func buildBackendKey(protectPath string, socketPath string, args []string) string {
	parts := make([]string, 0, len(args)+2)
	parts = append(parts, protectPath, socketPath)
	parts = append(parts, args...)
	return strings.Join(parts, "\x00")
}

func (b *sharedBackend) stop() {
	if b == nil {
		return
	}
	b.stopOnce.Do(func() {
		C.byedpi_stop()
	})
}

func (b *sharedBackend) cleanup() {
	if b != nil && b.socketPath != "" {
		_ = os.Remove(b.socketPath)
	}
}

func (b *sharedBackend) waitStopped(timeout time.Duration) bool {
	if b == nil {
		return true
	}
	select {
	case <-b.done:
		return true
	case <-time.After(timeout):
		return false
	}
}
