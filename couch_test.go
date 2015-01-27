package couch

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

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
		return nil, errors.New("Incorrect url: " + req.URL.String())
	}
	m.hdrs = req.Header
	return &http.Response{
		Body:       ioutil.NopCloser(bytes.NewReader(m.res)),
		Status:     "200 OK",
		StatusCode: m.rc,
	}, nil
}

func installClient(c *http.Client) {
	HTTPClient = c
}

func TestUnmarshalURLGolden(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 200, nil}

	installClient(&http.Client{Transport: &m})

	idr := idAndRev{}
	err := unmarshalURL(u, &idr)
	if err != nil {
		t.Fatalf("Error unmarshaling: %v", err)
	}

	if idr.ID != "theid" || idr.Rev != "therev" {
		t.Fatalf("Expected theid/therev, got %v", idr)
	}
}

func TestUnmarshURLError(t *testing.T) {
	err := unmarshalURL("http://%", nil)
	if err == nil {
		t.Fatalf("Successfully unmarshalled from nothing?")
	} else if !strings.Contains(err.Error(), "hexadecimal escape") {
		t.Fatalf("Unexpected error: %q", err.Error())
	}
}

func TestUnmarshSchemeError(t *testing.T) {
	err := unmarshalURL("mailto:dustin@arpa.in", nil)
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

	idr := idAndRev{}
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

	if idr.ID != "theid" || idr.Rev != "therev" {
		t.Fatalf("Expected theid/therev, got %v", idr)
	}
}

func TestInteractBadResp(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 419, nil}

	installClient(&http.Client{Transport: &m})

	idr := idAndRev{}
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

type fakeHTTP struct {
	responses []http.Response
}

func (f *fakeHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	if len(f.responses) == 0 {
		return &http.Response{
			Status:     "Server Error",
			StatusCode: 500,
			Body:       ioutil.NopCloser(&bytes.Buffer{}),
		}, nil
	}
	p := http.Response(f.responses[0])
	f.responses = f.responses[1:]
	return &p, nil
}

func installFakeHTTP(f *fakeHTTP) *http.Client {
	rv := HTTPClient
	HTTPClient = &http.Client{Transport: f}
	return rv
}

func uninstallFakeHTTP(h *http.Client) {
	HTTPClient = h
}

func oneFake(r http.Response) *fakeHTTP {
	return &fakeHTTP{[]http.Response{r}}
}

func TestUnmarshalBadReq(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 404,
		Status:     "404 four-oh-four",
		Body:       ioutil.NopCloser(&bytes.Buffer{}),
	})))

	err := unmarshalURL("http://www.example.com/", nil)
	if err == nil {
		t.Fatalf("Successfully got example?")
	} else if !strings.Contains(err.Error(), "four-oh-four") {
		t.Fatalf("Unexpected error: %q", err.Error())
	}
}

func TestRunningSuccess(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"version":"CouchDB"}`)),
	})))
	d := Database{}
	if !d.Running() {
		t.Fatalf("Expected DB to be considered running.  Wasn't.")
	}
}

func TestRunningEmpty(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{}`)),
	})))
	d := Database{}
	if d.Running() {
		t.Fatalf("Expected DB to be considered not running.  Was.")
	}
}

func TestDBExists(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "x"}`)),
	})))
	d := Database{Name: "x"}
	if !d.Exists() {
		t.Errorf("Expected DB to exist.  Didn't.")
	}

	installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "y"}`)),
	}))
	if d.Exists() {
		t.Errorf("Expected DB to not exist.  Did.")
	}

	installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "`)),
	}))
	if d.Exists() {
		t.Errorf("Expected DB to not exist.  Did.")
	}
}

func TestRunningError(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`[`)),
	})))
	d := Database{}
	if d.Running() {
		t.Fatalf("Expected DB to be considered not running.  Was.")
	}
}

func TestSimpleOpFail(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 500,
		Status:     "five hundred",
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": false}`)),
	})))
	d := Database{}
	err := d.simpleOp("PUT", "/x", io.EOF)
	if !strings.Contains(err.Error(), "five hundred") {
		t.Fatalf("Expected error, got %v", err)
	}
}

func TestSimpleOpNotOK(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": false}`)),
	})))
	d := Database{}
	if err := d.simpleOp("PUT", "/x", io.EOF); err != io.EOF {
		t.Fatalf("Expected error, got %v", err)
	}
}

func TestSimpleOpOK(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": true}`)),
	})))
	d := Database{}
	if err := d.simpleOp("PUT", "/x", io.EOF); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestCreateDB(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": true}`)),
	})))
	d := Database{}
	if err := d.createDatabase(); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestDeleteDB(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"ok": true}`)),
	})))
	d := Database{}
	if err := d.DeleteDatabase(); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestURLs(t *testing.T) {
	h := map[string][]string{}
	tests := []struct {
		db  Database
		exp string
	}{
		{Database{"locohost", "5984", "dbx", nil,
			h, nil, defaultChangeDelay},
			"http://locohost:5984/dbx"},
		{Database{"locohost", "5984", "dbx", url.UserPassword("a", "b"),
			h, nil, defaultChangeDelay},
			"http://a:b@locohost:5984/dbx"},
	}
	for _, test := range tests {
		if test.db.DBURL() != test.exp {
			t.Errorf("Error on %v, expected %v, got %v",
				test.db, test.exp, test.db.DBURL())
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
	j, id, rev, err := cleanJSON(struct {
		Key string
		ID  string `json:"_id"`
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

	j, id, rev, err = cleanJSON(map[string]string{
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
	j, id, rev, err := cleanJSON(map[string]string{
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
	j, id, rev, err := cleanJSON(map[string]interface{}{
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
	j, id, rev, err := cleanJSON(map[string]interface{}{
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
	j, id, rev, err := cleanJSON(map[string]string{
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
	j, id, rev, err := cleanJSON(make(chan bool))
	if err == nil {
		t.Errorf("Expected error encoding chan, got %s (id=%v, rev=%v)",
			j, id, rev)
	}
}

func TestBulk(t *testing.T) {
	hres := `[{"ok": true, "id": "d1"},{"ok": true, "id": "d2"}]`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	docs := []interface{}{
		map[string]string{"k": "v"},
		map[string]string{"k2": "v2"},
	}
	res, err := d.Bulk(docs)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	exp := []Response{
		Response{Ok: true, ID: "d1"},
		Response{Ok: true, ID: "d2"},
	}

	if !reflect.DeepEqual(exp, res) {
		t.Errorf("Expected %v, got %v", exp, res)
	}
}

func TestBulkBadInput(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: -1,
		Body:       ioutil.NopCloser(&bytes.Buffer{}),
	})))
	d := Database{}
	docs := []interface{}{make(chan bool)}
	res, err := d.Bulk(docs)
	if err == nil {
		t.Fatalf("Expected error, got %v", res)
	}
}

func TestPrivateInsertError(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 419, nil}

	installClient(&http.Client{Transport: &m})

	d := Database{}
	id, rev, err := d.insert(nil)
	if err == nil {
		t.Fatalf("Expected error 419, got: %v/%v/%v", id, rev, err)
	}
}

func TestPrivateInsertNotOK(t *testing.T) {
	hres := `{"ok": false, "error": "Broken", "reason": "Because"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	id, res, err := d.insert(nil)
	if err == nil {
		t.Fatalf("Expected error, got %v/%v", id, res)
	}
}

func TestPrivateInsertOK(t *testing.T) {
	hres := `{"ok": true, "id": "one", "rev": "11"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	id, rev, err := d.insert(nil)
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	if id != "one" {
		t.Errorf(`Expected id="one", got %v`, id)
	}
	if rev != "11" {
		t.Errorf(`Expected rev="11", got %v`, rev)
	}

}

func TestPrivateInsertWithError(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 419, nil}

	installClient(&http.Client{Transport: &m})

	d := Database{}
	id, rev, err := d.insertWith(nil, "x")
	if err == nil {
		t.Fatalf("Expected error 419, got: %v/%v/%v", id, rev, err)
	}
}

func TestPrivateInsertWithNotOK(t *testing.T) {
	hres := `{"ok": false, "error": "Broken", "reason": "Because"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	id, res, err := d.insertWith(nil, "x")
	if err == nil {
		t.Fatalf("Expected error, got %v/%v", id, res)
	}
}

func TestPrivateInsertWithOK(t *testing.T) {
	hres := `{"ok": true, "id": "one", "rev": "11"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	id, rev, err := d.insertWith(nil, "x")
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	if id != "one" {
		t.Errorf(`Expected id="one", got %v`, id)
	}
	if rev != "11" {
		t.Errorf(`Expected rev="11", got %v`, rev)
	}
}

func TestRetrieveInvalid(t *testing.T) {
	d := Database{}
	err := d.Retrieve("", nil)
	if err != errNoID {
		t.Fatalf("Expected 'no ID' error, got %v", err)
	}
}

func TestRetrieveValid(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:5984/thing/ob"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev", "val": "EX"}`), 200, nil}

	installClient(&http.Client{Transport: &m})

	s := struct{ Val string }{}
	d := Database{Host: "localhost", Port: "5984", Name: "thing"}
	err := d.Retrieve("ob", &s)
	if err != nil {
		t.Fatalf("Error unmarshaling: %v", err)
	}

	if s.Val != "EX" {
		t.Fatalf("Expected EX, got %v", s.Val)
	}
}

func TestInsertBadOb(t *testing.T) {
	d := Database{}
	id, rev, err := d.Insert(make(chan bool))
	if err == nil {
		t.Fatalf("Unexpected success on bad obj: %v/%v", id, rev)
	}
}

func TestInsertNew(t *testing.T) {
	hres := `{"ok": true, "id": "one", "rev": "11"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))

	d := Database{}
	id, rev, err := d.Insert(map[string]interface{}{})
	if err != nil {
		t.Fatalf("Errored: %v", err)
	}
	if id != "one" {
		t.Errorf(`Expected id="one", got %q`, id)
	}
	if rev != "11" {
		t.Errorf(`Expected rev="11", got %q`, id)
	}
}

func TestInsertWith(t *testing.T) {
	hres := `{"ok": true, "id": "one", "rev": "11"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))

	d := Database{}
	id, rev, err := d.Insert(map[string]interface{}{"_id": "one"})
	if err != nil {
		t.Fatalf("Errored: %v", err)
	}
	if id != "one" {
		t.Errorf(`Expected id="one", got %q`, id)
	}
	if rev != "11" {
		t.Errorf(`Expected rev="11", got %q`, id)
	}
}

func TestInsertWithBadInput(t *testing.T) {
	d := Database{}
	id, rev, err := d.InsertWith(make(chan bool), "x")
	if err == nil {
		t.Errorf("Unexpected success: %v/%v", id, rev)
	}
}

// This is confusing, but there's Insert as insertWith and lower
// insertWith and public InsertWith
func TestInsertWith2(t *testing.T) {
	hres := `{"ok": true, "id": "one", "rev": "11"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))

	d := Database{}
	id, rev, err := d.InsertWith(map[string]interface{}{}, "one")
	if err != nil {
		t.Fatalf("Errored: %v", err)
	}
	if id != "one" {
		t.Errorf(`Expected id="one", got %q`, id)
	}
	if rev != "11" {
		t.Errorf(`Expected rev="11", got %q`, id)
	}
}

func TestInsertWithRev(t *testing.T) {
	hres := `{"ok": true, "id": "one", "rev": "11"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))

	d := Database{}
	id, rev, err := d.Insert(map[string]interface{}{"_id": "one",
		"_rev": "10"})
	if err != nil {
		t.Fatalf("Errored: %v", err)
	}
	if id != "one" {
		t.Errorf(`Expected id="one", got %q`, id)
	}
	if rev != "11" {
		t.Errorf(`Expected rev="11", got %q`, id)
	}
}

func TestDBInfo(t *testing.T) {
	hres := `{"db_name": "testdb", "doc_count": 38515}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))

	d := Database{}
	info, err := d.GetInfo()
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	if info.Name != "testdb" {
		t.Errorf(`Expected name="testdb", got %q`, info.Name)
	}
	if info.DocCount != 38515 {
		t.Errorf(`Expected count=38515, got %q`, info.DocCount)
	}
}

func TestDeleteError(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 419, nil}

	installClient(&http.Client{Transport: &m})

	d := Database{}
	id, rev, err := d.insert(nil)
	if err == nil {
		t.Fatalf("Expected error 419, got: %v/%v/%v", id, rev, err)
	}
}

func TestDeleteNotOK(t *testing.T) {
	hres := `{"ok": false, "error": "Broken", "reason": "Because"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	id, res, err := d.insert(nil)
	if err == nil {
		t.Fatalf("Expected error, got %v/%v", id, res)
	}
}

func TestDeleteWithError(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`), 419, nil}

	installClient(&http.Client{Transport: &m})

	d := Database{}
	err := d.Delete("x", "11")
	if err == nil {
		t.Fatalf("Expected error 419, got success")
	}
}

func TestDeleteWithNotOK(t *testing.T) {
	hres := `{"ok": false, "error": "Broken", "reason": "Because"}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	err := d.Delete("x", "11")
	if err == nil {
		t.Fatalf("Expected error")
	}
}

func TestDeleteWithOK(t *testing.T) {
	hres := `{"ok": true}`
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))
	d := Database{}
	err := d.Delete("x", "11")
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
}

func TestNewDBNotRunning(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`[`)),
	})))
	db, err := NewDatabase("localhost", "5984", "db")
	if err == nil {
		t.Fatalf("Expected failure, got %v", db)
	}
}

func TestNewDBCreateFail(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(&fakeHTTP{
		responses: []http.Response{
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`["x"]`)),
			},
		},
	}))
	db, err := NewDatabase("localhost", "5984", "db")
	if err == nil {
		t.Fatalf("Expected failure, got %v", db)
	}
}

func TestNewDBCreateExists(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(&fakeHTTP{
		responses: []http.Response{
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"version":"CouchDB"}`)),
			},
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "db"}`)),
			},
		},
	}))
	_, err := NewDatabase("localhost", "5984", "db")
	if err != nil {
		t.Fatalf("Expected succcess, got %v", err)
	}
}

func TestNewDBCreateSuccess(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(&fakeHTTP{
		responses: []http.Response{
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"version":"CouchDB"}`)),
			},
			http.Response{
				StatusCode: 404,
				Body:       ioutil.NopCloser(strings.NewReader(``)),
			},
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"ok": true}`)),
			},
		},
	}))
	_, err := NewDatabase("localhost", "5984", "db")
	if err != nil {
		t.Fatalf("Expected succcess, got %v", err)
	}
}

func TestEditBadOb(t *testing.T) {
	d := Database{}
	id, err := d.Edit(make(chan bool))
	if err == nil {
		t.Fatalf("Expected error on bad input, got %v", id)
	}
}

func TestEditNoID(t *testing.T) {
	d := Database{}
	id, err := d.Edit(map[string]interface{}{})
	if err != errNoID {
		t.Fatalf("Expected no ID err on bad input, got %v/%v", err, id)
	}
}

func TestEditNoRev(t *testing.T) {
	d := Database{}
	id, err := d.Edit(map[string]interface{}{"_id": "theid"})
	if err != errNoRev {
		t.Fatalf("Expected no rev error on bad input, got %v/%v", err, id)
	}
}

func TestEditHTTPFail(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 500,
		Body:       ioutil.NopCloser(strings.NewReader(``)),
	})))

	d := Database{}
	id, err := d.Edit(map[string]interface{}{"_id": "theid", "_rev": "84"})
	if err == nil {
		t.Fatalf("Expected http error, got %v", id)
	}
}

func TestEditHTTPSuccess(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"rev": "85"}`)),
	})))

	d := Database{}
	id, err := d.Edit(map[string]interface{}{"_id": "theid", "_rev": "84"})
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	if id != "85" {
		t.Fatalf(`Expected rev="85", got %q`, id)
	}
}

func TestEditWithNoID(t *testing.T) {
	d := Database{}
	id, err := d.EditWith(map[string]interface{}{}, "", "r")
	if err != errNoID {
		t.Fatalf("Expected no ID err on bad input, got %v/%v", err, id)
	}
}

func TestEditWithNoRev(t *testing.T) {
	d := Database{}
	id, err := d.EditWith(map[string]interface{}{}, "x", "")
	if err != errNoRev {
		t.Fatalf("Expected no rev error on bad input, got %v/%v", err, id)
	}
}

func TestEditWithBadInput(t *testing.T) {
	d := Database{}
	id, err := d.EditWith(make(chan bool), "a", "3")
	if err == nil {
		t.Fatalf("Expected error with bad input, got %v", id)
	}
}

func TestEditWithHTTPSuccess(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"rev": "85"}`)),
	})))

	d := Database{}
	id, err := d.EditWith(map[string]interface{}{}, "x", "84")
	if err != nil {
		t.Fatalf("Expected success, got %v", err)
	}
	if id != "85" {
		t.Fatalf(`Expected rev="85", got %q`, id)
	}
}

func TestConnectBadURL(t *testing.T) {
	d, err := Connect("http://%")
	if err == nil {
		t.Fatalf("Expected error connecting with bad URL, got %v", d)
	}
}

func TestConnectNotRunning(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(&fakeHTTP{}))
	db, err := Connect("http://localhost:5984/")
	if err != errNotRunning {
		t.Fatalf("Expected error with bad connection, got %v/%v", db, err)
	}
}

func TestConnectNoDB(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(&fakeHTTP{
		responses: []http.Response{
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`["db"]`)),
			},
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`[]`)),
			},
		},
	}))

	db, err := Connect("http://localhost:5984/db")
	if err == nil {
		t.Fatalf("Expected error with no db, got %v", db)
	}
}

func TestConnectSuccess(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(&fakeHTTP{
		responses: []http.Response{
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"version":"CouchDB"}`)),
			},
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "db"}`)),
			},
		},
	}))

	db, err := Connect("http://localhost:5984/db")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if db.Port != "5984" {
		t.Fatalf("Expected port 5984, got %q", db.Port)
	}
}

func TestConnectSuccessDefaultPort(t *testing.T) {
	defer uninstallFakeHTTP(installFakeHTTP(&fakeHTTP{
		responses: []http.Response{
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"version":"CouchDB"}`)),
			},
			http.Response{
				StatusCode: 200,
				Body:       ioutil.NopCloser(strings.NewReader(`{"db_name": "db"}`)),
			},
		},
	}))

	db, err := Connect("http://localhost/db")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if db.Port != "80" {
		t.Fatalf("Expected port 80, got %q", db.Port)
	}
}
