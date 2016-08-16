package couch

import (
	"io"
	"net"
	"testing"
	"time"
)

type testRC struct {
	bytes, reads int
	err          error
}

func (t *testRC) Read(b []byte) (int, error) {
	t.reads++
	t.bytes += len(b)
	return len(b), t.err
}

func (t *testRC) Close() error {
	t.err = io.EOF
	return nil
}

type testDeadliner int

func (t *testDeadliner) SetReadDeadline(time.Time) error {
	*t++
	return nil
}

func TestTimeoutClient(t *testing.T) {
	trc := &testRC{}
	var td testDeadliner
	tc := timeoutClient{trc, &td, 13}
	buf := make([]byte, 4096)

	_, err := tc.Read(buf)
	if err != nil {
		t.Fatalf("Failed first read: %v", err)
	}
	tc.Close()
	_, err = tc.Read(buf)
	if err == nil {
		t.Fatalf("Didn't fail second read")
	}

	if trc.reads != 2 || trc.bytes != 8192 {
		t.Errorf("Expected %v reads at %v bytes, got %v / %v",
			2, 8912, trc.reads, trc.bytes)
	}
}

func TestI64Opt(t *testing.T) {
	m := map[string]interface{}{
		"a": 1,
		"b": int64(2),
		"c": 3.14,
		"d": "4",
		"e": "five",
		"f": TestI64Opt,
	}

	tests := map[string]int64{
		"a": 1,
		"b": 2,
		"c": 3,
		"d": 4,
		"e": 99,
		"f": 99,
	}

	for k, exp := range tests {
		got := i64defopt(m, k, 99)
		if got != exp {
			t.Errorf("Expected %v for %v (%v), got %v",
				exp, k, m[k], got)
		}
	}
}

type mockConn struct {
	stuff   []byte
	waiting chan bool
	fail    bool
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.fail {
		m.fail = false
		return 0, io.EOF
	}
	<-m.waiting
	if len(m.stuff) == 0 {
		return 0, io.EOF
	}
	n := copy(b, m.stuff)
	m.stuff = m.stuff[n:]
	return n, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	select {
	case <-m.waiting:
	default:
		close(m.waiting)
	}
	return len(b), err
}

func (m *mockConn) Close() error {
	return nil
}

func (m mockConn) LocalAddr() net.Addr {
	return nil
}

func (m mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func mockDialer(m *mockConn) func(string, string) (net.Conn, error) {
	return func(string, string) (net.Conn, error) {
		return m, nil
	}
}

func makeEmptyMock() func(string, string) (net.Conn, error) {
	mock := &mockConn{[]byte(`HTTP/1.0 200 OK

`), make(chan bool), true}
	return mockDialer(mock)
}

func TestChangesTwice(t *testing.T) {
	d := Database{
		changesDialer:    makeEmptyMock(),
		changesFailDelay: 5,
		Host:             "localhost",
	}
	err := d.Changes(func(io.Reader) int64 { return -1 }, map[string]interface{}{})
	t.Logf("Error: %v", err)
}

func TestChangesWithOptions(t *testing.T) {
	d := Database{
		changesDialer:    makeEmptyMock(),
		changesFailDelay: 5,
		Host:             "localhost",
	}
	err := d.Changes(func(io.Reader) int64 { return -1 },
		map[string]interface{}{
			"since":     858245,
			"start_key": "x",
			"heartbeat": 3999,
		})
	t.Logf("Error: %v", err)
}

func TestChangesWithNegativeHB(t *testing.T) {
	d := Database{
		changesDialer:    makeEmptyMock(),
		changesFailDelay: 5,
		Host:             "localhost",
	}
	err := d.Changes(func(io.Reader) int64 { return -1 },
		map[string]interface{}{
			"since":     858245,
			"start_key": "x",
			"heartbeat": -3999,
		})
	t.Logf("Error: %v", err)
}
