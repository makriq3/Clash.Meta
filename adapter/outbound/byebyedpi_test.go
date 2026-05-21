package outbound

import (
	"bytes"
	"net"
	"testing"
	"time"
)

type recordingConn struct {
	bytes.Buffer
	writes [][]byte
}

func (c *recordingConn) Write(b []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), b...))
	return c.Buffer.Write(b)
}

func (c *recordingConn) Read(b []byte) (int, error)       { return 0, nil }
func (c *recordingConn) Close() error                     { return nil }
func (c *recordingConn) LocalAddr() net.Addr              { return nil }
func (c *recordingConn) RemoteAddr() net.Addr             { return nil }
func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

func TestByeByeDPIPlanParsesPositionalDesyncFlagsAsSafeSplits(t *testing.T) {
	plan := parseByeByeDPIPlan([]string{"-o1", "-d3+s", "--tlsrec=5+s"})
	want := []int{1, 3, 5}
	if len(plan.splits) != len(want) {
		t.Fatalf("unexpected split count: %v", plan.splits)
	}
	for i := range want {
		if plan.splits[i] != want[i] {
			t.Fatalf("unexpected splits: %v", plan.splits)
		}
	}
}

func TestByeByeDPIConnSplitsFirstWriteOnly(t *testing.T) {
	rec := &recordingConn{}
	conn := &byeByeDPIConn{
		Conn: rec,
		plan: byeByeDPIDesyncPlan{splits: []int{1, 3}},
	}
	n, err := conn.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("unexpected write size: %d", n)
	}
	_, _ = conn.Write([]byte("!"))
	gotWrites := [][]byte{[]byte("h"), []byte("el"), []byte("lo"), []byte("!")}
	if len(rec.writes) != len(gotWrites) {
		t.Fatalf("unexpected writes: %q", rec.writes)
	}
	for i := range gotWrites {
		if !bytes.Equal(rec.writes[i], gotWrites[i]) {
			t.Fatalf("write %d = %q, want %q", i, rec.writes[i], gotWrites[i])
		}
	}
}
