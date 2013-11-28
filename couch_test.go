// -*- tab-width: 4 -*-
package couch

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestHttpError(t *testing.T) {
	err := HttpError{404, "four oh four"}
	if err.Error() != "four oh four" {
		t.Errorf(`Expected "four oh four", got %q`, err.Error())
	}
}

func tGetCreds(r *http.Request) (string, string) {
	ah := r.Header.Get("Authorization")
	if ah == "" {
		return "", ""
	}
	parts := strings.Fields(ah)
	if len(parts) < 2 {
		return "", ""
	}
	dec, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ""
	}
	parts = strings.SplitN(string(dec), ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func TestReqGen(t *testing.T) {
	tests := []struct {
		in         string
		user, pass string
		fails      bool
	}{
		{"http://localhost:5984/%", "", "", true},
		{"http://localhost:5984/", "", "", false},
		{"http://me:secret@localhost:5984/", "me", "secret", false},
	}

	for _, test := range tests {
		req, err := createReq(test.in)
		switch {
		case err == nil && !test.fails:
			u, p := tGetCreds(req)
			if u != test.user || p != test.pass {
				t.Errorf("Expected user=%q, pass=%q, got %q/%q",
					test.user, test.pass, u, p)
			}
		case err == nil && test.fails:
			t.Errorf("Expected failure on %q, got %v", test.in, req)
		case err != nil && !test.fails:
			t.Errorf("Unexpected failure on %q: %v", test.in, err)
		}
	}
}

type mocktrip struct {
	expurl string
	res    []byte
	rc     int
	hdrs   http.Header
}

func (m *mocktrip) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.expurl != req.URL.String() {
		return nil, &HttpError{400, "Incorrect url: " + req.URL.String()}
	}
	m.hdrs = req.Header
	return &http.Response{
		Body:       ioutil.NopCloser(bytes.NewReader(m.res)),
		Status:     "200 OK",
		StatusCode: m.rc,
	}, nil
}

func installClient(c *http.Client) {
	HttpClient = c
}

func TestUnmarshalURLGolden(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 200, nil}

	installClient(&http.Client{Transport: &m})

	idr := IdAndRev{}
	err := unmarshal_url(u, &idr)
	if err != nil {
		t.Fatalf("Error unmarshaling: %v", err)
	}

	if idr.Id != "theid" || idr.Rev != "therev" {
		t.Fatalf("Expected theid/therev, got %v", idr)
	}
}

func TestUnmarshURLError(t *testing.T) {
	err := unmarshal_url("http://%", nil)
	if err == nil {
		t.Fatalf("Successfully unmarshalled from nothing?")
	} else if !strings.Contains(err.Error(), "hexadecimal escape") {
		t.Fatalf("Unexpected error: %q", err.Error())
	}
}

func TestUnmarshSchemeError(t *testing.T) {
	err := unmarshal_url("mailto:dustin@arpa.in", nil)
	if err == nil {
		t.Fatalf("Successfully unmarshalled from nothing?")
	} else if !strings.Contains(err.Error(), "unsupported protocol") {
		t.Fatalf("Unexpected error: %q", err.Error())
	}
}

func TestInteractGolden(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 200, nil}

	installClient(&http.Client{Transport: &m})

	idr := IdAndRev{}
	n, err := interact("POST", u, map[string][]string{"X-What": []string{"a"}},
		[]byte{'{', '}'}, &idr)
	if n != 200 || err != nil {
		t.Fatalf("Error unmarshaling: %v/%v", n, err)
	}

	if m.hdrs.Get("Content-Type") != "application/json" {
		t.Errorf("Expected JSON header, got %q", m.hdrs.Get("Content-Type"))
	}
	if m.hdrs.Get("X-What") != "a" {
		t.Errorf("Expected custom header, got %q\n%v", m.hdrs.Get("X-What"), m.hdrs)
	}

	if idr.Id != "theid" || idr.Rev != "therev" {
		t.Fatalf("Expected theid/therev, got %v", idr)
	}
}

func TestInteractBadResp(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 419, nil}

	installClient(&http.Client{Transport: &m})

	idr := IdAndRev{}
	n, err := interact("POST", u, map[string][]string{}, []byte{'{', '}'}, &idr)
	if n != 419 || err == nil {
		t.Fatalf("Expected error 419, got: %v/%v", n, err)
	}
}

func TestInteractError(t *testing.T) {
	_, err := interact("POST", "http://%", map[string][]string{}, nil, nil)
	if err == nil {
		t.Fatalf("Successfully interacted with nothing?")
	} else if !strings.Contains(err.Error(), "hexadecimal escape") {
		t.Fatalf("Unexpected error: %q", err.Error())
	}
}

func TestInteractSchemeError(t *testing.T) {
	_, err := interact("POST", "mailto:dustin@arpa.in", map[string][]string{}, nil, nil)
	if err == nil {
		t.Fatalf("Successfully interacted with nothing?")
	} else if !strings.Contains(err.Error(), "unsupported protocol") {
		t.Fatalf("Unexpected error: %q", err.Error())
	}
}

type fakeHttp http.Response

func (f fakeHttp) RoundTrip(*http.Request) (*http.Response, error) {
	p := http.Response(f)
	return &p, nil
}

func installFakeHttp(f fakeHttp) *http.Client {
	rv := HttpClient
	HttpClient = &http.Client{Transport: f}
	return rv
}

func uninstallFakeHttp(h *http.Client) {
	HttpClient = h
}

func TestUnmarshalBadReq(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 404,
		Status:     "404 four-oh-four",
		Body:       ioutil.NopCloser(&bytes.Buffer{}),
	}))

	err := unmarshal_url("http://www.example.com/", nil)
	if err == nil {
		t.Fatalf("Successfully got example?")
	} else if !strings.Contains(err.Error(), "four-oh-four") {
		t.Fatalf("Unexpected error: %q", err.Error())
	}
}

func TestRunningSuccess(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`["adb"]`)),
	}))
	d := Database{}
	if !d.Running() {
		t.Fatalf("Expected DB to be considered running.  Wasn't.")
	}
}

func TestRunningEmpty(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`[]`)),
	}))
	d := Database{}
	if d.Running() {
		t.Fatalf("Expected DB to be considered not running.  Was.")
	}
}

func TestDBExists(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "x"}`)),
	}))
	d := Database{Name: "x"}
	if !d.Exists() {
		t.Errorf("Expected DB to exist.  Didn't.")
	}

	installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "y"}`)),
	})
	if d.Exists() {
		t.Errorf("Expected DB to not exist.  Did.")
	}

	installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "`)),
	})
	if d.Exists() {
		t.Errorf("Expected DB to not exist.  Did.")
	}
}

func TestRunningError(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`[`)),
	}))
	d := Database{}
	if d.Running() {
		t.Fatalf("Expected DB to be considered not running.  Was.")
	}
}

func TestSimpleOpFail(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 500,
		Status:     "five hundred",
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": false}`)),
	}))
	d := Database{}
	if err := d.simpleOp("PUT", "/x", io.EOF); err.Error() != "five hundred" {
		t.Fatalf("Expected error, got %v", err)
	}
}

func TestSimpleOpNotOK(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": false}`)),
	}))
	d := Database{}
	if err := d.simpleOp("PUT", "/x", io.EOF); err != io.EOF {
		t.Fatalf("Expected error, got %v", err)
	}
}

func TestSimpleOpOK(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": true}`)),
	}))
	d := Database{}
	if err := d.simpleOp("PUT", "/x", io.EOF); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestCreateDB(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": true}`)),
	}))
	d := Database{}
	if err := d.create_database(); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestDeleteDB(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(fakeHttp{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": true}`)),
	}))
	d := Database{}
	if err := d.DeleteDatabase(); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestURLs(t *testing.T) {
	tests := []struct {
		db  Database
		exp string
	}{
		{Database{"locohost", "5984", "dbx", nil},
			"http://locohost:5984/dbx"},
		{Database{"locohost", "5984", "dbx", url.UserPassword("a", "b")},
			"http://a:b@locohost:5984/dbx"},
	}
	for _, test := range tests {
		if test.db.DBURL() != test.exp {
			t.Errorf("Error on %v, expected %v, got %v",
				test.db, test.exp, test.db.DBURL())
		}
	}
}

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

func TestMust(t *testing.T) {
	must(nil) // no panic
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		must(io.EOF)
	}()
	if !panicked {
		t.Fatalf("Expected a panic, but didn't get one")
	}
}

func TestCleanJSON(t *testing.T) {
	j, id, rev, err := clean_JSON(struct {
		Key string
		Id  string `json:"_id"`
		Rev string `json:"_rev"`
	}{"aqui", "aid", "arev"})
	m := map[string]interface{}{}
	err = json.Unmarshal(j, &m)
	if err != nil {
		t.Fatalf("Error in struct literal parsing: %v", err)
	}
	if id != "aid" {
		t.Errorf(`Expected id="aid", got %q`, id)
	}
	if rev != "arev" {
		t.Errorf(`Expected rev="arev", got %q`, rev)
	}
	if len(m) != 1 {
		t.Errorf("Expected one key, got %v", m)
	}

	j, id, rev, err = clean_JSON(map[string]string{
		"Key":  "anotherkey",
		"_id":  "mid",
		"_rev": "mrev"})

	m = map[string]interface{}{}
	err = json.Unmarshal(j, &m)
	if err != nil {
		t.Fatalf("Error in struct map parsing: %v", err)
	}
	if id != "mid" {
		t.Errorf(`Expected id="mid", got %q`, id)
	}
	if rev != "mrev" {
		t.Errorf(`Expected rev="mrev", got %q`, rev)
	}
	if len(m) != 1 {
		t.Errorf("Expected one key, got %v", m)
	}
}

func TestCleanJSONNoRev(t *testing.T) {
	j, id, rev, err := clean_JSON(map[string]string{
		"Key": "third",
		"_id": "timid"})

	m := map[string]interface{}{}
	err = json.Unmarshal(j, &m)
	if err != nil {
		t.Fatalf("Error in struct map parsing: %v", err)
	}
	if id != "timid" {
		t.Errorf(`Expected id="timid", got %q`, id)
	}
	if rev != "" {
		t.Errorf(`Expected empty rev, got %q`, rev)
	}
	if len(m) != 1 {
		t.Errorf("Expected one key, got %v", m)
	}

}

func TestCleanJSONNonStringID(t *testing.T) {
	j, id, rev, err := clean_JSON(map[string]interface{}{
		"Key": "third",
		"_id": 3.141592})

	m := map[string]interface{}{}
	err = json.Unmarshal(j, &m)
	if err != nil {
		t.Fatalf("Error in struct map parsing: %v", err)
	}
	if id != "" {
		t.Errorf(`Expected empty id, got %q`, id)
	}
	if rev != "" {
		t.Errorf(`Expected empty rev, got %q`, rev)
	}
	if len(m) != 1 {
		t.Errorf("Expected one key, got %v", m)
	}

}

func TestCleanJSONNonStringRev(t *testing.T) {
	j, id, rev, err := clean_JSON(map[string]interface{}{
		"Key":  "third",
		"_rev": 3.141592})

	m := map[string]interface{}{}
	err = json.Unmarshal(j, &m)
	if err != nil {
		t.Fatalf("Error in struct map parsing: %v", err)
	}
	if id != "" {
		t.Errorf(`Expected empty id, got %q`, id)
	}
	if rev != "" {
		t.Errorf(`Expected empty rev, got %q`, rev)
	}
	if len(m) != 1 {
		t.Errorf("Expected one key, got %v", m)
	}

}

func TestCleanJSONNoId(t *testing.T) {
	j, id, rev, err := clean_JSON(map[string]string{
		"Key":  "third",
		"_rev": "theengine"})

	m := map[string]interface{}{}
	err = json.Unmarshal(j, &m)
	if err != nil {
		t.Fatalf("Error in struct map parsing: %v", err)
	}
	if id != "" {
		t.Errorf(`Expected id="", got %q`, id)
	}
	if rev != "theengine" {
		t.Errorf(`Expected rev="theengine", got %q`, rev)
	}
	if len(m) != 1 {
		t.Errorf("Expected one key, got %v", m)
	}

}

func TestCleanJSONError(t *testing.T) {
	// error
	j, id, rev, err := clean_JSON(make(chan bool))
	if err == nil {
		t.Errorf("Expected error encoding chan, got %s (id=%v, rev=%v)",
			j, id, rev)
	}
}
