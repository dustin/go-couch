// -*- tab-width: 4 -*-
package couch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

var def_hdrs = map[string][]string{}

type buffer struct {
	b *bytes.Buffer
}

func (b *buffer) Read(out []byte) (int, error) {
	return b.b.Read(out)
}

func (b *buffer) Close() error { return nil }

// Converts given URL to string containing the body of the response.
func url_to_buf(u string) []byte {
	if r, err := http.Get(u); err == nil {
		b, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err == nil {
			return b
		}
	}
	return make([]byte, 0)
}

func unmarshal_url(u string, results interface{}) error {
	if r, err := http.Get(u); err == nil {
		defer r.Body.Close()

		d := json.NewDecoder(r.Body)
		if err := d.Decode(results); err != nil {
			return err
		}
	} else {
		return err
	}
	return nil
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
func (p Database) interact(method, u string, headers map[string][]string, in []byte, out interface{}) (int, error) {
	fullHeaders := map[string][]string{}
	for k, v := range headers {
		fullHeaders[k] = v
	}
	bodyLength := 0
	if in != nil {
		bodyLength = len(in)
		fullHeaders["Content-Type"] = []string{"application/json"}
	}
	req := http.Request{
		Method:        method,
		ProtoMajor:    1,
		ProtoMinor:    1,
		Close:         true,
		ContentLength: int64(bodyLength),
		Header:        fullHeaders,
	}
	req.TransferEncoding = []string{"chunked"}
	req.URL, _ = url.Parse(u)
	if in != nil {
		req.Body = &buffer{bytes.NewBuffer(in)}
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", p.Host, p.Port))
	if err != nil {
		return 0, err
	}
	http_conn := httputil.NewClientConn(conn, nil)
	defer http_conn.Close()
	if err := http_conn.Write(&req); err != nil {
		return 0, err
	}
	r, err := http_conn.Read(&req)
	if err != nil {
		return 0, err
	}
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		b := []byte{}
		r.Body.Read(b)
		return r.StatusCode, errors.New("server said: " + r.Status)
	}
	decoder := json.NewDecoder(r.Body)
	if err = decoder.Decode(out); err != nil {
		return 0, err
	}
	r.Body.Close()
	return r.StatusCode, nil
}

type Database struct {
	Host string
	Port string
	Name string
}

func (p Database) BaseURL() string {
	return fmt.Sprintf("http://%s:%s", p.Host, p.Port)
}

func (p Database) DBURL() string {
	return fmt.Sprintf("%s/%s", p.BaseURL(), p.Name)
}

// Test whether CouchDB is running (ignores Database.Name)
func (p Database) Running() bool {
	u := fmt.Sprintf("%s/%s", p.BaseURL(), "_all_dbs")
	s := url_to_buf(u)
	if len(s) > 0 {
		return true
	}
	return false
}

type database_info struct {
	Db_name string `json:"db_name"`
	// other stuff too, ignore for now
}

// Test whether specified database exists in specified CouchDB instance
func (p Database) Exists() bool {
	di := new(database_info)
	if err := unmarshal_url(p.DBURL(), &di); err != nil {
		return false
	}
	if di.Db_name != p.Name {
		return false
	}
	return true
}

func (p Database) create_database() error {
	ir := response{}
	if _, err := p.interact("PUT", p.DBURL(), def_hdrs, nil, &ir); err != nil {
		return err
	}
	if !ir.Ok {
		return errors.New("Create database operation returned not-OK")
	}
	return nil
}

// Deletes the given database and all documents
func (p Database) DeleteDatabase() error {
	ir := response{}
	if _, err := p.interact("DELETE", p.DBURL(), def_hdrs, nil, &ir); err != nil {
		return err
	}
	if !ir.Ok {
		return errors.New("Delete database operation returned not-OK")
	}
	return nil
}

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

	db := Database{host, port, u.Path[1:]}
	if !db.Running() {
		return Database{}, errors.New("CouchDB not running")
	}
	if !db.Exists() {
		return Database{}, errors.New("Database does not exist.")
	}

	return db, nil
}

func NewDatabase(host, port, name string) (Database, error) {
	db := Database{host, port, name}
	if !db.Running() {
		return db, errors.New("CouchDB not running")
	}
	if !db.Exists() {
		if err := db.create_database(); err != nil {
			return db, err
		}
	}
	return db, nil
}

// Strip _id and _rev from d, returning them separately if they exist
func clean_JSON(d interface{}) (json_buf []byte, id, rev string, err error) {
	json_buf, err = json.Marshal(d)
	if err != nil {
		return
	}
	m := map[string]interface{}{}
	err = json.Unmarshal(json_buf, &m)
	if err != nil {
		return
	}
	id_rev := new(IdAndRev)
	err = json.Unmarshal(json_buf, &id_rev)
	if err != nil {
		return
	}
	if _, ok := m["_id"]; ok {
		id = id_rev.Id
		delete(m, "_id")
	}
	if _, ok := m["_rev"]; ok {
		rev = id_rev.Rev
		delete(m, "_rev")
	}
	json_buf, err = json.Marshal(m)
	return
}

type response struct {
	Ok     bool
	Id     string
	Rev    string
	Error  string
	Reason string
}

// Inserts document to CouchDB, returning id and rev on success.
// Document may specify both "_id" and "_rev" fields (will overwrite existing)
//	or just "_id" (will use that id, but not overwrite existing)
//	or neither (will use autogenerated id)
func (p Database) Insert(d interface{}) (string, string, error) {
	json_buf, id, rev, err := clean_JSON(d)
	if err != nil {
		return "", "", err
	}
	if id != "" && rev != "" {
		new_rev, err2 := p.Edit(d)
		return id, new_rev, err2
	} else if id != "" {
		return p.insert_with(json_buf, id)
	} else if id == "" {
		return p.insert(json_buf)
	}
	return "", "", errors.New("invalid Document")
}

// Private implementation of simple autogenerated-id insert
func (p Database) insert(json_buf []byte) (string, string, error) {
	ir := response{}
	if _, err := p.interact("POST", p.DBURL(), def_hdrs, json_buf, &ir); err != nil {
		return "", "", err
	}
	if !ir.Ok {
		return "", "", errors.New(fmt.Sprintf("%s: %s", ir.Error, ir.Reason))
	}
	return ir.Id, ir.Rev, nil
}

// Inserts the given document (shouldn't contain "_id" or "_rev" tagged fields)
// using the passed 'id' as the _id. Will fail if the id already exists.
func (p Database) InsertWith(d interface{}, id string) (string, string, error) {
	json_buf, err := json.Marshal(d)
	if err != nil {
		return "", "", err
	}
	return p.insert_with(json_buf, id)
}

// Private implementation of insert with given id
func (p Database) insert_with(json_buf []byte, id string) (string, string, error) {
	u := fmt.Sprintf("%s/%s", p.DBURL(), url.QueryEscape(id))
	ir := response{}
	if _, err := p.interact("PUT", u, def_hdrs, json_buf, &ir); err != nil {
		return "", "", err
	}
	if !ir.Ok {
		return "", "", errors.New(fmt.Sprintf("%s: %s", ir.Error, ir.Reason))
	}
	return ir.Id, ir.Rev, nil
}

// Edits the given document, returning the new revision.
// d must contain "_id" and "_rev" tagged fields.
func (p Database) Edit(d interface{}) (string, error) {
	json_buf, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	id_rev := new(IdAndRev)
	err = json.Unmarshal(json_buf, id_rev)
	if err != nil {
		return "", err
	}
	if id_rev.Id == "" {
		return "", errors.New("Id not specified in interface")
	}
	if id_rev.Rev == "" {
		return "", errors.New("Rev not specified in interface (try InsertWith)")
	}
	u := fmt.Sprintf("%s/%s", p.DBURL(), url.QueryEscape(id_rev.Id))
	ir := response{}
	if _, err = p.interact("PUT", u, def_hdrs, json_buf, &ir); err != nil {
		return "", err
	}
	return ir.Rev, nil
}

// Edits the given document, returning the new revision.
// d should not contain "_id" or "_rev" tagged fields. If it does, they will
// be overwritten with the passed values.
func (p Database) EditWith(d interface{}, id, rev string) (string, error) {
	if id == "" || rev == "" {
		return "", errors.New("EditWith: must specify both id and rev")
	}
	json_buf, err := json.Marshal(d)
	if err != nil {
		return "", err
	}
	m := map[string]interface{}{}
	err = json.Unmarshal(json_buf, &m)
	if err != nil {
		return "", err
	}
	m["_id"] = id
	m["_rev"] = rev
	return p.Edit(m)
}

// Unmarshals the document matching id to the given interface
func (p Database) Retrieve(id string, d interface{}) error {
	if id == "" {
		return errors.New("no id specified")
	}

	return unmarshal_url(fmt.Sprintf("%s/%s", p.DBURL(), id), d)
}

// Deletes document given by id and rev.
func (p Database) Delete(id, rev string) error {
	headers := map[string][]string{
		"If-Match": []string{rev},
	}
	u := fmt.Sprintf("%s/%s", p.DBURL(), id)
	ir := response{}
	if _, err := p.interact("DELETE", u, headers, nil, &ir); err != nil {
		return err
	}
	if !ir.Ok {
		return errors.New(fmt.Sprintf("%s: %s", ir.Error, ir.Reason))
	}
	return nil
}

type Row struct {
	Id  *string
	Key *string
}

type keyed_view_response struct {
	Total_rows uint64
	Offset     uint64
	Rows       []Row
}

// Return array of document ids as returned by the given view/options combo.
// view should be eg. "_design/my_foo/_view/my_bar"
// options should be eg. { "limit": 10, "key": "baz" }
func (p Database) QueryIds(view string, options map[string]interface{}) ([]string, error) {
	kvr := new(keyed_view_response)

	if err := p.Query(view, options, kvr); err != nil {
		return make([]string, 0), err
	}

	ids := make([]string, len(kvr.Rows))
	i := 0
	for _, row := range kvr.Rows {
		if row.Id != nil {
			ids[i] = *row.Id
			i++
		}
	}
	return ids[:i], nil
}

func (p Database) Query(view string, options map[string]interface{}, results interface{}) error {
	if view == "" {
		return errors.New("empty view")
	}
	parameters := ""
	for k, v := range options {
		switch t := v.(type) {
		case string:
			parameters += fmt.Sprintf(`%s="%s"&`, k, url.QueryEscape(t))
		case int:
			parameters += fmt.Sprintf(`%s=%d&`, k, t)
		case bool:
			parameters += fmt.Sprintf(`%s=%v&`, k, t)
		default:
			// TODO more types are supported
			panic(fmt.Sprintf("unsupported value-type %T in Query", t))
		}
	}
	full_url := fmt.Sprintf("%s/%s?%s", p.DBURL(), view, parameters)

	return unmarshal_url(full_url, results)
}
