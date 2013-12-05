package couch

import (
	"io"
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
