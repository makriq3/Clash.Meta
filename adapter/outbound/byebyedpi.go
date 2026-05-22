package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/loopback"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/transport/byedpi"
)

const (
	byeByeDPIStrategyAuto      = "auto"
	byeByeDPIStrategyFixed     = "fixed"
	byeByeDPIStrategyBenchmark = "benchmark"
)

var defaultByeByeDPIArgs = []string{"-Ku", "-a1", "-An", "-o1", "-At,r,s", "-d1"}
var defaultByeByeDPITestSites = []string{"https://www.gstatic.com/generate_204"}
var defaultByeByeDPIStrategies = []ByeByeDPIStrategy{
	{Name: "default", Args: []string{"-Ku", "-a1", "-An", "-o1", "-At,r,s", "-d1"}},
	{Name: "split-basic", Args: []string{"-s1", "-d1", "-a1", "-At,r,s"}},
	{Name: "split-sni", Args: []string{"-s1+s", "-d3+s", "-a1", "-At,r,s"}},
	{Name: "tlsrec", Args: []string{"-r1+s", "-s1", "-a1", "-As"}},
	{Name: "oob-split", Args: []string{"-o1", "-s4", "-s6", "-a1"}},
	{Name: "disoob-split", Args: []string{"-q1", "-r25+s", "-a1"}},
	{Name: "multi-split", Args: []string{"-d1", "-s3+s", "-a1"}},
	{Name: "fake-compatible", Args: []string{"-f-1", "-r1+s", "-a1"}},
}

type ByeByeDPI struct {
	*Base
	option   *ByeByeDPIOption
	dialer   C.Dialer
	loopBack *loopback.Detector

	selectedMu sync.RWMutex
	selected   []string

	benchmarkOnce sync.Once
	benchmarkErr  error

	backendMu sync.Mutex
	backend   *byedpi.Instance
}

type ByeByeDPIOption struct {
	BasicOption
	Name       string              `proxy:"name"`
	Strategy   string              `proxy:"strategy,omitempty"`
	UDP        bool                `proxy:"udp,omitempty"`
	Args       []string            `proxy:"args,omitempty"`
	Strategies []ByeByeDPIStrategy `proxy:"strategies,omitempty"`
	Auto       ByeByeDPIAutoOption `proxy:"auto,omitempty"`
	RawOptions map[string]any      `proxy:"-"`
}

type ByeByeDPIStrategy struct {
	Name string   `proxy:"name,omitempty"`
	Args []string `proxy:"args,omitempty"`
}

type ByeByeDPIAutoOption struct {
	Mode        string   `proxy:"mode,omitempty"`
	TestSites   []string `proxy:"test-sites,omitempty"`
	Timeout     int      `proxy:"timeout,omitempty"`
	Requests    int      `proxy:"requests,omitempty"`
	Concurrency int      `proxy:"concurrency,omitempty"`
}

func NewByeByeDPI(option ByeByeDPIOption) (*ByeByeDPI, error) {
	if option.Name == "" {
		return nil, errors.New("missing name")
	}
	if option.Strategy == "" {
		option.Strategy = byeByeDPIStrategyAuto
	}
	if option.Auto.Mode == "" {
		option.Auto.Mode = "passive"
	}
	if len(option.Args) == 0 {
		option.Args = append([]string(nil), defaultByeByeDPIArgs...)
	}
	if len(option.Strategies) == 0 {
		option.Strategies = cloneByeByeDPIStrategies(defaultByeByeDPIStrategies)
	}
	if err := validateByeByeDPIOption(option); err != nil {
		return nil, err
	}
	option.UDP = false
	outbound := &ByeByeDPI{
		Base: &Base{
			name:   option.Name,
			addr:   "local",
			tp:     C.ByeByeDPI,
			pdName: option.ProviderName,
			udp:    false,
			tfo:    option.TFO,
			mpTcp:  option.MPTCP,
			iface:  option.Interface,
			rmark:  option.RoutingMark,
			prefer: option.IPVersion,
		},
		option:   &option,
		loopBack: loopback.NewDetector(),
	}
	outbound.dialer = option.NewDialer(outbound.DialOptions())
	outbound.selected = outbound.selectInitialStrategy()
	return outbound, nil
}

func validateByeByeDPIOption(option ByeByeDPIOption) error {
	if option.UDP {
		return errors.New("byebyedpi UDP is not supported with private socket backend")
	}
	switch option.Strategy {
	case byeByeDPIStrategyAuto, byeByeDPIStrategyFixed, byeByeDPIStrategyBenchmark:
	default:
		return fmt.Errorf("unsupported byebyedpi strategy: %s", option.Strategy)
	}
	switch option.Auto.Mode {
	case "", "passive", "benchmark", "both":
	default:
		return fmt.Errorf("unsupported byebyedpi auto mode: %s", option.Auto.Mode)
	}
	if option.Strategy == byeByeDPIStrategyFixed && len(option.Args) == 0 {
		return errors.New("byebyedpi fixed strategy requires args")
	}
	for _, args := range [][]string{option.Args} {
		if err := validateByeByeDPIArgs(args); err != nil {
			return err
		}
	}
	for _, strategy := range option.Strategies {
		if len(strategy.Args) == 0 {
			return errors.New("byebyedpi strategy entry requires args")
		}
		if err := validateByeByeDPIArgs(strategy.Args); err != nil {
			return err
		}
	}
	if option.Auto.Timeout < 0 || option.Auto.Requests < 0 || option.Auto.Concurrency < 0 {
		return errors.New("byebyedpi auto values must be non-negative")
	}
	return nil
}

func cloneByeByeDPIStrategies(strategies []ByeByeDPIStrategy) []ByeByeDPIStrategy {
	out := make([]ByeByeDPIStrategy, 0, len(strategies))
	for _, strategy := range strategies {
		out = append(out, ByeByeDPIStrategy{
			Name: strategy.Name,
			Args: append([]string(nil), strategy.Args...),
		})
	}
	return out
}

func validateByeByeDPIArgs(args []string) error {
	for i, arg := range args {
		switch {
		case arg == "-i" || arg == "--ip" || arg == "-p" || arg == "--port":
			return fmt.Errorf("byebyedpi listener option %q is not allowed", arg)
		case strings.HasPrefix(arg, "-i") && arg != "-I":
			return fmt.Errorf("byebyedpi listener option %q is not allowed", arg)
		case strings.HasPrefix(arg, "-p") && arg != "-P":
			return fmt.Errorf("byebyedpi listener option %q is not allowed", arg)
		case strings.HasPrefix(arg, "--ip=") || strings.HasPrefix(arg, "--port=") || arg == "-z" || strings.HasPrefix(arg, "-z") || arg == "--unix-socket" || strings.HasPrefix(arg, "--unix-socket="):
			return fmt.Errorf("byebyedpi listener option %q is not allowed", arg)
		case arg == "-P" || arg == "--protect-path" || strings.HasPrefix(arg, "-P") || strings.HasPrefix(arg, "--protect-path="):
			return fmt.Errorf("byebyedpi protect option %q is managed by Android core", arg)
		case arg == "-D" || arg == "--daemon" || arg == "-w" || arg == "--pidfile" || arg == "-E" || arg == "--transparent":
			return fmt.Errorf("byebyedpi process/listener option %q is not allowed", arg)
		case (arg == "-I" || arg == "--conn-ip") && i == len(args)-1:
			return fmt.Errorf("byebyedpi option %q requires a value", arg)
		}
	}
	return nil
}

func (b *ByeByeDPI) selectInitialStrategy() []string {
	if b.option.Strategy == byeByeDPIStrategyBenchmark && len(b.option.Strategies) > 0 {
		return append([]string(nil), b.option.Strategies[0].Args...)
	}
	return append([]string(nil), b.option.Args...)
}

func (b *ByeByeDPI) selectedArgs() []string {
	b.selectedMu.RLock()
	defer b.selectedMu.RUnlock()
	return append([]string(nil), b.selected...)
}

func (b *ByeByeDPI) setSelectedArgs(args []string) {
	b.selectedMu.Lock()
	defer b.selectedMu.Unlock()
	b.selected = append([]string(nil), args...)
}

func (b *ByeByeDPI) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if err := b.loopBack.CheckConn(metadata); err != nil {
		return nil, err
	}
	if err := b.ensureSelectedStrategy(ctx); err != nil {
		return nil, err
	}
	backend, err := b.ensureBackend()
	if err != nil {
		log.Warnln("[ByeByeDPI] %s backend start failed: %v", b.Name(), err)
		return nil, err
	}
	c, err := (&net.Dialer{}).DialContext(ctx, backend.Network(), backend.Addr())
	if err != nil {
		log.Warnln("[ByeByeDPI] %s backend dial failed: %v", b.Name(), err)
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = c.Close()
		}
	}()
	if _, err = b.socksBackend(backend.Network(), backend.Addr()).StreamConnContext(ctx, c, metadata); err != nil {
		log.Warnln("[ByeByeDPI] %s SOCKS relay failed: %v", b.Name(), err)
		return nil, err
	}
	return b.loopBack.NewConn(NewConn(c, b)), nil
}

func (b *ByeByeDPI) ensureSelectedStrategy(ctx context.Context) error {
	if !b.shouldBenchmark() {
		return nil
	}
	b.benchmarkOnce.Do(func() {
		b.benchmarkErr = b.runBenchmark(ctx)
	})
	return b.benchmarkErr
}

func (b *ByeByeDPI) shouldBenchmark() bool {
	if len(b.option.Strategies) == 0 {
		return false
	}
	return b.option.Strategy == byeByeDPIStrategyBenchmark ||
		(b.option.Strategy == byeByeDPIStrategyAuto && (b.option.Auto.Mode == "benchmark" || b.option.Auto.Mode == "both"))
}

func (b *ByeByeDPI) runBenchmark(ctx context.Context) error {
	sites := b.option.Auto.TestSites
	if len(sites) == 0 {
		sites = defaultByeByeDPITestSites
	}
	timeout := time.Duration(b.option.Auto.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	requests := b.option.Auto.Requests
	if requests <= 0 {
		requests = 1
	}

	type result struct {
		args  []string
		score int
	}
	best := result{args: b.selectedArgs(), score: -1}
	for _, strategy := range b.option.Strategies {
		score := 0
		for _, site := range sites {
			for i := 0; i < requests; i++ {
				if b.checkSite(ctx, site, strategy.Args, timeout) {
					score++
				}
			}
		}
		if score > best.score {
			best = result{args: strategy.Args, score: score}
		}
	}
	if len(best.args) > 0 {
		b.setSelectedArgs(best.args)
	}
	return nil
}

func (b *ByeByeDPI) checkSite(ctx context.Context, site string, args []string, timeout time.Duration) bool {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	transport := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ResponseHeaderTimeout: timeout,
		TLSHandshakeTimeout:   timeout,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			backend, err := byedpi.Start(args)
			if err != nil {
				return nil, err
			}
			c, err := (&net.Dialer{}).DialContext(ctx, backend.Network(), backend.Addr())
			if err != nil {
				_ = backend.Close()
				return nil, err
			}
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				_ = backend.Close()
				_ = c.Close()
				return nil, err
			}
			p, _ := strconv.Atoi(port)
			metadata := &C.Metadata{Host: host, DstPort: uint16(p)}
			if _, err = b.socksBackend(backend.Network(), backend.Addr()).StreamConnContext(ctx, c, metadata); err != nil {
				_ = backend.Close()
				_ = c.Close()
				return nil, err
			}
			return closeWithBackendConn{Conn: c, backend: backend}, nil
		},
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, site, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Connection", "close")
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

func (b *ByeByeDPI) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	return nil, C.ErrNotSupport
}

func (b *ByeByeDPI) IsL3Protocol(metadata *C.Metadata) bool {
	return true
}

func (b *ByeByeDPI) ProxyInfo() C.ProxyInfo {
	info := b.Base.ProxyInfo()
	info.DialerProxy = b.option.DialerProxy
	return info
}

func (b *ByeByeDPI) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"type":     b.Type().String(),
		"id":       b.Id(),
		"strategy": b.option.Strategy,
		"udp":      false,
	})
}

func (b *ByeByeDPI) ensureBackend() (*byedpi.Instance, error) {
	b.backendMu.Lock()
	defer b.backendMu.Unlock()
	if b.backend != nil {
		return b.backend, nil
	}
	backend, err := byedpi.Start(b.selectedArgs())
	if err != nil {
		return nil, err
	}
	b.backend = backend
	log.Infoln("[ByeByeDPI] %s backend started", b.Name())
	return backend, nil
}

func (b *ByeByeDPI) socksBackend(network string, addr string) *Socks5 {
	return &Socks5{
		Base: &Base{
			name:   b.Name(),
			addr:   addr,
			tp:     C.ByeByeDPI,
			pdName: b.ProxyInfo().ProviderName,
			udp:    b.option.UDP,
			dialer: localByeByeDPIDialer{network: network},
		},
		option: &Socks5Option{Name: b.Name(), Server: "127.0.0.1", UDP: false},
	}
}

func (b *ByeByeDPI) Close() error {
	b.backendMu.Lock()
	defer b.backendMu.Unlock()
	if b.backend != nil {
		err := b.backend.Close()
		b.backend = nil
		return err
	}
	return nil
}

type closeWithBackendConn struct {
	net.Conn
	backend *byedpi.Instance
}

func (c closeWithBackendConn) Close() error {
	err := c.Conn.Close()
	if c.backend != nil {
		if backendErr := c.backend.Close(); err == nil {
			err = backendErr
		}
	}
	return err
}

type localByeByeDPIDialer struct {
	network string
}

func (d localByeByeDPIDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.network != "" {
		network = d.network
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func (localByeByeDPIDialer) ListenPacket(ctx context.Context, network, address string, rAddrPort netip.AddrPort) (net.PacketConn, error) {
	return (&net.ListenConfig{}).ListenPacket(ctx, network, address)
}
