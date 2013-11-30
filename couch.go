// -*- tab-width: 4 -*-

// Package couch provides a CouchDB API.
package couch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var defaultHdrs = map[string][]string{}

// HTTP Client used by typical requests.
//
// Defaults to http.DefaultClient
var HttpClient = http.DefaultClient

// HttpError represents errors returned from unsuccessful HTTP requests.
type HttpError struct {
	Status int
	Msg    string
}

func (e *HttpError) Error() string {
	return e.Msg
}

func createReq(u string) (*http.Request, error) {
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	if req.URL.User != nil {
		if p, hasp := req.URL.User.Password(); hasp {
			req.SetBasicAuth(req.URL.User.Username(), p)
		}
	}
	return req, nil
}

func unmarshalURL(u string, results interface{}) error {
	req, err := createReq(u)
	if err != nil {
		return err
	}

	r, err := HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	if r.StatusCode != 200 {
		return &HttpError{r.StatusCode, r.Status}
	}

	return json.NewDecoder(r.Body).Decode(results)
}

type IdAndRev struct {
	Id  string `json:"_id"`
	Rev string `json:"_rev"`
}

// Sends a query to CouchDB and parses the response back.
// method: the name of the HTTP method (POST, PUT,...)
// url: the URL to interact with
// headers: additional headers to pass to the request
// in: body of the request
// out: a structure to fill in with the returned JSON document
func interact(method, u string, headers map[string][]string, in []byte, out interface{}) (int, error) {
	fullHeaders := map[string][]string{}
	for k, v := range headers {
		fullHeaders[k] = v
	}
	if in != nil {
		fullHeaders["Content-Type"] = []string{"application/json"}
	}

	req, err := http.NewRequest(method, u, bytes.NewReader(in))
	if err != nil {
		return 0, err
	}

	req.ContentLength = int64(len(in))
	req.Header = fullHeaders
	req.Close = true

	res, err := HttpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return res.StatusCode, &HttpError{res.StatusCode, res.Status}
	}
	return res.StatusCode, json.NewDecoder(res.Body).Decode(out)
}

type Database struct {
	Host     string
	Port     string
	Name     string
	authinfo *url.Userinfo
}

func (p Database) BaseURL() string {
	if p.authinfo == nil {
		return fmt.Sprintf("http://%s:%s", p.Host, p.Port)
	}
	return fmt.Sprintf("http://%s@%s:%s", p.authinfo.String(), p.Host, p.Port)
}

func (p Database) DBURL() string {
	return fmt.Sprintf("%s/%s", p.BaseURL(), p.Name)
}

// Test whether CouchDB is running (ignores Database.Name)
func (p Database) Running() bool {
	dbs := []string{}
	u := fmt.Sprintf("%s/%s", p.BaseURL(), "_all_dbs")
	return unmarshalURL(u, &dbs) == nil && len(dbs) > 0
}

type databaseInfo struct {
	DBName string `json:"db_name"`
	// other stuff too, ignore for now
}

// Test whether specified database exists in specified CouchDB instance
func (p Database) Exists() bool {
	di := &databaseInfo{}
	return unmarshalURL(p.DBURL(), &di) == nil && di.DBName == p.Name
}

func (p Database) simpleOp(method, url string, nokerr error) error {
	ir := Response{}
	if _, err := interact(method, url, defaultHdrs, nil, &ir); err != nil {
		return err
	}
	if !ir.Ok {
		return nokerr
	}
	return nil
}

var (
	errNewDB = errors.New("create database operation returned not-OK")
	errDelDB = errors.New("delete database operation returned not-OK")
)

func (p Database) createDatabase() error {
	return p.simpleOp("PUT", p.DBURL(), errNewDB)
}

// Deletes the given database and all documents
func (p Database) DeleteDatabase() error {
	return p.simpleOp("DELETE", p.DBURL(), errDelDB)
}

var errNotRunning = errors.New("CouchDB not running")

// Connect to the database at the given URL.
// example:   couch.Connect("http://localhost:5984/testdb/")
func Connect(dburl string) (Database, error) {
	u, err := url.Parse(dburl)
	if err != nil {
		return Database{}, err
	}

	host := u.Host
	port := "80"
	if hp := strings.Split(u.Host, ":"); len(hp) > 1 {
		host = hp[0]
		port = hp[1]
	}

	db := Database{host, port, u.Path[1:], u.User}
	if !db.Running() {
		return Database{}, errNotRunning
	}
	if !db.Exists() {
		return Database{}, errors.New("database does not exist")
	}

	return db, nil
}

func NewDatabase(host, port, name string) (Database, error) {
	db := Database{host, port, name, nil}
	if !db.Running() {
		return db, errNotRunning
	}
	if !db.Exists() {
		if err := db.createDatabase(); err != nil {
			return db, err
		}
	}
	return db, nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// Strip _id and _rev from d, returning them separately if they exist
func cleanJSON(d interface{}) (jsonBuf []byte, id, rev string, err error) {
	jsonBuf, err = json.Marshal(d)
	if err != nil {
		return
	}
	m := map[string]interface{}{}
	must(json.Unmarshal(jsonBuf, &m))
	id, _ = m["_id"].(string)
	delete(m, "_id")
	rev, _ = m["_rev"].(string)
	delete(m, "_rev")
	jsonBuf, err = json.Marshal(m)
	return
}

type Response struct {
	Ok     bool
	Id     string
	Rev    string
	Error  string
	Reason string
}

// Bulk modification interface.
// Each item should be JSON serializable into a valid document.
// "_id" and "_rev" will be honored.
// To delete, add a "_deleted" field with a value of "true" as well
// as a valid "_rev" field.
func (p Database) Bulk(docs []interface{}) (results []Response, err error) {
	m := map[string]interface{}{}
	m["docs"] = docs
	var jsonBuf []byte
	jsonBuf, err = json.Marshal(m)
	if err != nil {
		return
	}

	results = make([]Response, 0, len(docs))
	_, err = interact("POST", p.DBURL()+"/_bulk_docs", defaultHdrs, jsonBuf, &results)
	return
}

// Inserts document to CouchDB, returning id and rev on success.
// Document may specify both "_id" and "_rev" fields (will overwrite existing)
//	or just "_id" (will use that id, but not overwrite existing)
//	or neither (will use autogenerated id)
func (p Database) Insert(d interface{}) (string, string, error) {
	jsonBuf, id, rev, err := cleanJSON(d)
	if err != nil {
		return "", "", err
	}
	if id != "" && rev != "" {
		new_rev, err2 := p.Edit(d)
		return id, new_rev, err2
	} else if id != "" {
		return p.insertWith(jsonBuf, id)
	} else if id == "" {
		return p.insert(jsonBuf)
	}
	return "", "", errors.New("invalid Document")
}

// Private implementation of simple autogenerated-id insert
func (p Database) insert(jsonBuf []byte) (string, string, error) {
	ir := Response{}
	if _, err := interact("POST", p.DBURL(), defaultHdrs, jsonBuf, &ir); err != nil {
		return "", "", err
	}
	if !ir.Ok {
		return "", "", fmt.Errorf("%s: %s", ir.Error, ir.Reason)
	}
	return ir.Id, ir.Rev, nil
}

// Inserts the given document (shouldn't contain "_id" or "_rev" tagged fields)
// using the passed 'id' as the _id. Will fail if the id already exists.
func (p Database) InsertWith(d interface{}, id string) (string, string, error) {
	jsonBuf, err := json.Marshal(d)
	if err != nil {
		return "", "", err
	}
	return p.insertWith(jsonBuf, id)
}

// Private implementation of insert with given id
func (p Database) insertWith(jsonBuf []byte, id string) (string, string, error) {
	u := fmt.Sprintf("%s/%s", p.DBURL(), url.QueryEscape(id))
	ir := Response{}
	if _, err := interact("PUT", u, defaultHdrs, jsonBuf, &ir); err != nil {
		return "", "", err
	}
	if !ir.Ok {
		return "", "", fmt.Errorf("%s: %s", ir.Error, ir.Reason)
	}
	return ir.Id, ir.Rev, nil
}

// Edits the given document, returning the new revision.
// d must contain "_id" and "_rev" tagged fields.
func (p Database) Edit(d interface{}) (string, error) {
	jsonBuf, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	id_rev := new(IdAndRev)
	err = json.Unmarshal(jsonBuf, id_rev)
	if err != nil {
		return "", err
	}
	if id_rev.Id == "" {
		return "", errors.New("id not specified in interface")
	}
	if id_rev.Rev == "" {
		return "", errors.New("rev not specified in interface (try InsertWith)")
	}
	u := fmt.Sprintf("%s/%s", p.DBURL(), url.QueryEscape(id_rev.Id))
	ir := Response{}
	if _, err = interact("PUT", u, defaultHdrs, jsonBuf, &ir); err != nil {
		return "", err
	}
	return ir.Rev, nil
}

// Edits the given document, returning the new revision.
// d should not contain "_id" or "_rev" tagged fields. If it does, they will
// be overwritten with the passed values.
func (p Database) EditWith(d interface{}, id, rev string) (string, error) {
	if id == "" || rev == "" {
		return "", errors.New("must specify both id and rev")
	}
	jsonBuf, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	m := map[string]interface{}{}
	err = json.Unmarshal(jsonBuf, &m)
	if err != nil {
		return "", err
	}
	m["_id"] = id
	m["_rev"] = rev
	return p.Edit(m)
}

var errNoID = errors.New("no id specified")

// Unmarshals the document matching id to the given interface
func (p Database) Retrieve(id string, d interface{}) error {
	if id == "" {
		return errNoID
	}

	return unmarshalURL(fmt.Sprintf("%s/%s", p.DBURL(), id), d)
}

// Deletes document given by id and rev.
func (p Database) Delete(id, rev string) error {
	headers := map[string][]string{
		"If-Match": []string{rev},
	}
	u := fmt.Sprintf("%s/%s", p.DBURL(), id)
	ir := Response{}
	if _, err := interact("DELETE", u, headers, nil, &ir); err != nil {
		return err
	}
	if !ir.Ok {
		return fmt.Errorf("%s: %s", ir.Error, ir.Reason)
	}
	return nil
}

type Row struct {
	Id  *string
	Key *string
}

// Result from GetInfo
type DBInfo struct {
	Name        string `json:"db_name"`
	DocCount    int64  `json:"doc_count"`
	DocDelCount int64  `json:"doc_del_count"`
	UpdateSeq   int64  `json:"update_seq"`
	PurgeSeq    int64  `json:"purge_seq"`
	Compacting  bool   `json:"compact_running"`
	DiskSize    int64  `json:"disk_size"`
	DataSize    int64  `json:"data_size"`
	StartTime   string `json:"instance_start_time"`
	Version     int    `json:"disk_format_version"`
	CommitedSeq int64  `json:"committed_update_seq"`
}

// Get the DBInfo for this database.
func (p Database) GetInfo() (DBInfo, error) {
	rv := DBInfo{}
	err := unmarshalURL(p.DBURL(), &rv)
	return rv, err
}

// Handle the stream of changes coming from a Changes thing.
//
// The handler returns the next sequence number when the stream should
// be resumed, otherwise -1 to indicate the changes feed should stop.
//
// The handler may return at any time to restart the stream from the
// sequence number in indicated in its return value.
type ChangeHandler func(r io.Reader) int64

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

// Feed the changes.
//
// The handler receives the body of the stream and is expected to consume
// the contents.
func (p Database) Changes(handler ChangeHandler,
	options map[string]interface{}) error {

	largest := i64defopt(options, "since", 0)

	heartbeatTime := i64defopt(options, "heartbeat", 5000)

	timeout := time.Minute
	if heartbeatTime > 0 {
		timeout = time.Millisecond * time.Duration(heartbeatTime*2)
	}

	for largest >= 0 {
		params := url.Values{}
		for k, v := range options {
			params.Set(k, fmt.Sprintf("%v", v))
		}
		if largest > 0 {
			params.Set("since", fmt.Sprintf("%v", largest))
		}

		if heartbeatTime > 0 {
			params.Set("heartbeat", fmt.Sprintf("%d", heartbeatTime))
		} else {
			params.Del("heartbeat")
		}

		full_url := fmt.Sprintf("%s/_changes?%s", p.DBURL(),
			params.Encode())

		var conn net.Conn

		// Swapping out the transport to work around a bug.
		client := &http.Client{Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: func(n, addr string) (net.Conn, error) {
				var err error
				conn, err = net.Dial(n, addr)
				return conn, err
			},
		}}

		resp, err := client.Get(full_url)
		if err == nil {
			func() {
				defer resp.Body.Close()
				defer conn.Close()

				tc := timeoutClient{resp.Body, conn, timeout}
				largest = handler(&tc)
			}()
		} else {
			log.Printf("Error in stream: %v", err)
			time.Sleep(time.Second * 1)
		}
	}
	return nil
}
