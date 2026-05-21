package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/component/dialer"
	"github.com/metacubex/mihomo/component/loopback"
	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
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

type byeByeDPIDesyncPlan struct {
	splits []int
}

type byeByeDPIConn struct {
	net.Conn
	once sync.Once
	plan byeByeDPIDesyncPlan
	err  error
}

func (c *byeByeDPIConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return c.Conn.Write(b)
	}
	usedPlan := false
	c.once.Do(func() {
		usedPlan = true
		c.err = c.writeWithPlan(b)
	})
	if usedPlan {
		if c.err != nil {
			return 0, c.err
		}
		return len(b), nil
	}
	return c.Conn.Write(b)
}

func (c *byeByeDPIConn) writeWithPlan(b []byte) error {
	splits := normalizeSplitPositions(c.plan.splits, len(b))
	if len(splits) == 0 {
		_, err := c.Conn.Write(b)
		return err
	}
	start := 0
	for _, split := range splits {
		if split <= start || split >= len(b) {
			continue
		}
		if _, err := c.Conn.Write(b[start:split]); err != nil {
			return err
		}
		start = split
	}
	if start < len(b) {
		_, err := c.Conn.Write(b[start:])
		return err
	}
	return nil
}

func normalizeSplitPositions(positions []int, size int) []int {
	if size <= 1 {
		return nil
	}
	seen := map[int]struct{}{}
	out := make([]int, 0, len(positions))
	for _, pos := range positions {
		if pos < 0 {
			pos = size + pos
		}
		if pos <= 0 || pos >= size {
			continue
		}
		if _, ok := seen[pos]; ok {
			continue
		}
		seen[pos] = struct{}{}
		out = append(out, pos)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
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
	outbound := &ByeByeDPI{
		Base: &Base{
			name:   option.Name,
			addr:   "local",
			tp:     C.ByeByeDPI,
			pdName: option.ProviderName,
			udp:    option.UDP,
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
		case strings.HasPrefix(arg, "--ip=") || strings.HasPrefix(arg, "--port="):
			return fmt.Errorf("byebyedpi listener option %q is not allowed", arg)
		case arg == "-D" || arg == "--daemon" || arg == "-w" || arg == "--pidfile" || arg == "-E" || arg == "--transparent":
			return fmt.Errorf("byebyedpi process/listener option %q is not allowed", arg)
		case (arg == "-I" || arg == "--conn-ip" || arg == "-P" || arg == "--protect-path") && i == len(args)-1:
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
	c, err := b.dialTCP(ctx, metadata.RemoteAddress())
	if err != nil {
		return nil, err
	}
	plan := parseByeByeDPIPlan(b.selectedArgs())
	return b.loopBack.NewConn(NewConn(&byeByeDPIConn{Conn: c, plan: plan}, b)), nil
}

func (b *ByeByeDPI) dialTCP(ctx context.Context, address string) (net.Conn, error) {
	if b.option.DialerProxy != "" || b.option.DialerForAPI != nil {
		return b.dialer.DialContext(ctx, "tcp", address)
	}
	opts := append(b.DialOptions(), dialer.WithResolver(resolver.DirectHostResolver))
	return dialer.DialContext(ctx, "tcp", address, opts...)
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
			c, err := b.dialTCP(ctx, address)
			if err != nil {
				return nil, err
			}
			return &byeByeDPIConn{Conn: c, plan: parseByeByeDPIPlan(args)}, nil
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
	if !b.SupportUDP() {
		return nil, C.ErrNotSupport
	}
	if err := b.loopBack.CheckPacketConn(metadata); err != nil {
		return nil, err
	}
	if err := b.ResolveUDP(ctx, metadata); err != nil {
		return nil, err
	}
	opts := append(b.DialOptions(), dialer.WithResolver(resolver.DirectHostResolver))
	pc, err := dialer.NewDialer(opts...).ListenPacket(ctx, "udp", "", metadata.AddrPort())
	if err != nil {
		return nil, err
	}
	return b.loopBack.NewPacketConn(newPacketConn(pc, b)), nil
}

func (b *ByeByeDPI) ResolveUDP(ctx context.Context, metadata *C.Metadata) error {
	if (!metadata.Resolved() || resolver.DirectHostResolver != resolver.DefaultResolver) && metadata.Host != "" {
		ip, err := resolver.ResolveIPWithResolver(ctx, metadata.Host, resolver.DirectHostResolver)
		if err != nil {
			return fmt.Errorf("can't resolve ip: %w", err)
		}
		metadata.DstIP = ip
	}
	return nil
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
		"udp":      b.option.UDP,
	})
}

func parseByeByeDPIPlan(args []string) byeByeDPIDesyncPlan {
	plan := byeByeDPIDesyncPlan{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var value string
		switch {
		case arg == "-s" || arg == "--split" ||
			arg == "-d" || arg == "--disorder" ||
			arg == "-o" || arg == "--oob" ||
			arg == "-q" || arg == "--disoob" ||
			arg == "-r" || arg == "--tlsrec":
			if i+1 >= len(args) {
				continue
			}
			i++
			value = args[i]
		case strings.HasPrefix(arg, "-s") && len(arg) > 2:
			value = arg[2:]
		case strings.HasPrefix(arg, "--split="):
			value = strings.TrimPrefix(arg, "--split=")
		case strings.HasPrefix(arg, "-d") && len(arg) > 2:
			value = arg[2:]
		case strings.HasPrefix(arg, "--disorder="):
			value = strings.TrimPrefix(arg, "--disorder=")
		case strings.HasPrefix(arg, "-o") && len(arg) > 2:
			value = arg[2:]
		case strings.HasPrefix(arg, "--oob="):
			value = strings.TrimPrefix(arg, "--oob=")
		case strings.HasPrefix(arg, "-q") && len(arg) > 2:
			value = arg[2:]
		case strings.HasPrefix(arg, "--disoob="):
			value = strings.TrimPrefix(arg, "--disoob=")
		case strings.HasPrefix(arg, "-r") && len(arg) > 2:
			value = arg[2:]
		case strings.HasPrefix(arg, "--tlsrec="):
			value = strings.TrimPrefix(arg, "--tlsrec=")
		default:
			continue
		}
		if pos, ok := parseByeByeDPIPosition(value); ok {
			plan.splits = append(plan.splits, pos)
		}
	}
	return plan
}

func parseByeByeDPIPosition(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	for _, sep := range []string{"+", ":", ","} {
		if idx := strings.Index(value, sep); idx >= 0 {
			value = value[:idx]
		}
	}
	pos, err := strconv.Atoi(value)
	if err != nil || pos == 0 {
		return 0, false
	}
	return pos, true
}
