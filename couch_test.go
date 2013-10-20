// -*- tab-width: 4 -*-
package couch

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
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
}

func (m mocktrip) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.expurl != req.URL.String() {
		return nil, &HttpError{400, "Incorrect url: " + req.URL.String()}
	}
	return &http.Response{
		Body:       ioutil.NopCloser(bytes.NewReader(m.res)),
		Status:     "200 OK",
		StatusCode: 200,
	}, nil
}

func installClient(c *http.Client) {
	HttpClient = c
}

func TestUnmarshalURLGolden(t *testing.T) {
	defer installClient(http.DefaultClient)

	u := "http://localhost:8654/thing"
	m := mocktrip{u, []byte(`{"_id": "theid", "_rev": "therev"}`)}

	installClient(&http.Client{Transport: m})

	idr := IdAndRev{}
	err := unmarshal_url(u, &idr)
	if err != nil {
		t.Fatalf("Error unmarshaling: %v", err)
	}

	if idr.Id != "theid" || idr.Rev != "therev" {
		t.Fatalf("Expected theid/therev, got %v", idr)
	}
}
