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
		shutdown(server_fd, SHUT_RDWR);
		close(server_fd);
		server_fd = -1;
	}
}
*/
import "C"

import (
	"errors"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
	key      string
	addr     string
	done     chan int
	refs     int
	stopOnce sync.Once
	stopped  atomic.Bool
	exitCode atomic.Int32
}

func Start(args []string) (*Instance, error) {
	protectPath := os.Getenv("BBDPI_PROTECT_PATH")
	if protectPath == "" && runtime.GOOS == "android" {
		return nil, errors.New("byedpi protect path is not configured")
	}
	key := buildBackendKey(protectPath, args)

	runMu.Lock()
	for {
		if current != nil && current.stopped.Load() {
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
		previous.waitStopped(2 * time.Second)
		if !previous.stopped.Load() {
			runMu.Lock()
			current = previous
			runMu.Unlock()
			return nil, errors.New("previous byedpi backend did not stop")
		}
		runMu.Lock()
	}

	port, err := reservePort()
	if err != nil {
		runMu.Unlock()
		return nil, err
	}
	fullArgs := []string{"ciadpi", "--ip", "127.0.0.1", "--port", strconv.Itoa(port)}
	if protectPath != "" {
		fullArgs = append(fullArgs, "--protect-path", protectPath)
	}
	fullArgs = append(fullArgs, args...)
	log.Infoln("[ByeByeDPI] starting native backend at %s, protect=%v, args=%d", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), protectPath != "", len(args))

	argc := C.int(len(fullArgs))
	argv := make([]*C.char, len(fullArgs))
	for i, arg := range fullArgs {
		argv[i] = C.CString(arg)
	}
	argv = append(argv, nil)
	done := make(chan int, 1)
	backend := &sharedBackend{
		key:  key,
		addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
		done: done,
		refs: 1,
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
		backend.waitStopped(2 * time.Second)
		runMu.Lock()
		if current == backend {
			current = nil
		}
		runMu.Unlock()
		log.Warnln("[ByeByeDPI] native backend did not become ready: %v", err)
		return nil, err
	}
	log.Infoln("[ByeByeDPI] native backend ready at %s", backend.addr)
	return inst, nil
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
	runMu.Unlock()
	i.backend = nil
	return nil
}

func reservePort() (int, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func (i *Instance) waitReady(timeout time.Duration) error {
	if i == nil || i.backend == nil {
		return errors.New("byedpi backend is not initialized")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp4", i.backend.addr, 100*time.Millisecond)
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

func buildBackendKey(protectPath string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, protectPath)
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

func (b *sharedBackend) waitStopped(timeout time.Duration) {
	if b == nil {
		return
	}
	select {
	case <-b.done:
	case <-time.After(timeout):
	}
}
