package couch

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// A ChangeHandler handles the stream of changes coming from Changes.
//
// The handler returns the next sequence number when the stream should
// be resumed, otherwise -1 to indicate the changes feed should stop.
//
// The handler may return at any time to restart the stream from the
// sequence number in indicated in its return value.
type ChangeHandler func(r io.Reader) interface{}

type ChangedRev struct {
	Revision string `json:"rev"`
}

type Change struct {
	Sequence    interface{}  `json:"seq"`
	Id          string       `json:"id"`
	ChangedRevs []ChangedRev `json:"changes"`
	Deleted     bool         `json:"deleted"`
}

type Changes struct {
	Results      []Change    `json:"results"`
	LastSequence interface{} `json:"last_seq"`
}

const defaultChangeDelay = time.Second

type timeoutClient struct {
	body       io.ReadCloser
	underlying interface {
		SetReadDeadline(time.Time) error
	}
	readTimeout time.Duration
}

func (tc *timeoutClient) Read(p []byte) (n int, err error) {
	if tc.readTimeout > 0 {
		tc.underlying.SetReadDeadline(time.Now().Add(tc.readTimeout))
	}
	return tc.body.Read(p)
}

func (tc *timeoutClient) Close() error {
	return tc.body.Close()
}

func i64defopt(opts map[string]interface{}, k string, def int64) int64 {
	rv := def

	if l, ok := opts[k]; ok {
		switch i := l.(type) {
		case int:
			rv = int64(i)
		case int64:
			rv = i
		case float64:
			rv = int64(i)
		case string:
			l, err := strconv.ParseInt(i, 10, 64)
			if err == nil {
				rv = l
			}
		default:
			log.Printf("Unknown type for '%s' param: %T", k, l)
		}
	}

	return rv
}

func ReadAllChanges(reader io.Reader) (Changes, error) {
	changes := Changes{}
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&changes)
	return changes, err

}

// Changes feeds a ChangeHandler a CouchDB changes feed.
//
// The handler receives the body of the stream and is expected to consume
// the contents.
func (p Database) Changes(handler ChangeHandler,
	options map[string]interface{}) error {

	since := options["since"]

	heartbeatTime := i64defopt(options, "heartbeat", 5000)

	timeout := time.Minute
	if heartbeatTime > 0 {
		timeout = time.Millisecond * time.Duration(heartbeatTime*2)
	}

	for {
		params := url.Values{}
		for k, v := range options {
			if v == nil {
				// skip any nil values, eg if "since" -> nil
				continue
			}
			params.Set(k, fmt.Sprintf("%v", v))
		}
		if since != nil {
			params.Set("since", fmt.Sprintf("%v", since))
		}

		if heartbeatTime > 0 {
			params.Set("heartbeat", fmt.Sprintf("%d", heartbeatTime))
		} else {
			params.Del("heartbeat")
		}

		fullURL := fmt.Sprintf("%s/_changes?%s", p.DBURL(),
			params.Encode())

		var conn net.Conn

		// Swapping out the transport to work around a bug.
		client := &http.Client{Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: func(n, addr string) (net.Conn, error) {
				var err error
				conn, err = p.changesDialer(n, addr)
				return conn, err
			},
		}}

		resp, err := client.Get(fullURL)
		if err == nil {
			func() {
				defer resp.Body.Close()
				defer conn.Close()

				tc := timeoutClient{resp.Body, conn, timeout}
				since = handler(&tc)
			}()
			if since == nil {
				// handler wants to end changes feed
				break
			}

		} else {
			log.Printf("Error in stream: %v", err)
			time.Sleep(p.changesFailDelay)
		}
	}
	return nil
}
