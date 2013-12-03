package couch

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestQueryNoView(t *testing.T) {
	d := Database{}
	err := d.Query("", nil, nil)
	if err != errEmptyView {
		t.Errorf("Expected empty view error, got %v", err)
	}
}

func TestQueryIdsNoView(t *testing.T) {
	d := Database{}
	x, err := d.QueryIds("", nil)
	if err != errEmptyView {
		t.Errorf("Expected empty view error, got %v/%v", x, err)
	}
}

func TestViewURL(t *testing.T) {
	vpath := "_design/testview/_view/v"

	// Unmarshallable parameter
	d := Database{Host: "localhost", Port: "5984"}
	u, err := d.ViewURL(vpath, map[string]interface{}{"ch": make(chan bool)})
	if err == nil {
		t.Errorf("Expected error on unmarshalable param, got %v", u)
	}

	tests := []struct {
		params map[string]interface{}
		exp    map[string]string
	}{
		{map[string]interface{}{"i": 1, "b": true, "s": "ess"},
			map[string]string{"i": "1", "b": "true", "s": `"ess"`}},
		{map[string]interface{}{"unk": DocId("le"), "startkey_docid": "ess"},
			map[string]string{"unk": "le", "startkey_docid": "ess"}},
		{map[string]interface{}{"stale": "update_after"},
			map[string]string{"stale": "update_after"}},
		{map[string]interface{}{"startkey": []string{"a"}},
			map[string]string{"startkey": `["a"]`}},
	}

	for _, test := range tests {
		us, err := d.ViewURL(vpath, test.params)
		if err != nil {
			t.Errorf("Failed on %v: %v", test, err)
			continue
		}

		u, err := url.Parse(us)
		if err != nil {
			t.Errorf("Failed on %v", test)
			continue
		}

		got := u.Query()

		if len(got) != len(test.exp) {
			t.Errorf("Expected %v, got %v", test.exp, got)
			continue
		}

		for k, v := range test.exp {
			if len(got[k]) != 1 || got.Get(k) != v {
				t.Errorf("Expected param %v to be %q on %v, was %#q",
					k, v, test, got[k])
			}
		}
	}
}

func TestBadViewParam(t *testing.T) {
	d := Database{Host: "localhost", Port: "5984"}
	thing, err := d.ViewURL("aview", map[string]interface{}{
		"aparam": make(chan bool),
	})
	if err == nil {
		t.Errorf("Failed to build a view with a bad param, got %v",
			thing)
	}
}

func TestQueryBadViewParam(t *testing.T) {
	d := Database{Host: "localhost", Port: "5984"}
	ob := map[string]interface{}{}
	err := d.Query("aview", map[string]interface{}{
		"aparam": make(chan bool),
	}, &ob)
	if err == nil {
		t.Errorf("Failed to build a view with a bad param, got %v", ob)
	}
}

func TestQuerySuccess(t *testing.T) {
	defer uninstallFakeHttp(installFakeHttp(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(`{"k": "v"}`)),
	})))

	d := Database{Host: "localhost", Port: "5984"}
	ob := map[string]interface{}{}
	err := d.Query("aview", map[string]interface{}{}, &ob)
	if err != nil {
		t.Errorf("Failed to execute a view: %v", err)
	}
	if ob["k"] != "v" {
		t.Fatalf("Expected v, got %q", ob["k"])
	}
}

func TestQueryIDsSuccess(t *testing.T) {
	hres := `{"rows": [{"id": "one"}, {}, {"id": "three"}]}`
	defer uninstallFakeHttp(installFakeHttp(oneFake(http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(strings.NewReader(hres)),
	})))

	d := Database{Host: "localhost", Port: "5984"}
	ids, err := d.QueryIds("aview", map[string]interface{}{})
	if err != nil {
		t.Errorf("Failed to execute a view: %v", err)
	}
	if len(ids) != 2 || ids[0] != "one" || ids[1] != "three" {
		t.Fatalf("Didn't get expected IDs: %v", ids)
	}

}
