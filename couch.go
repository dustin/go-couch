// -*- tab-width: 4 -*-
package couch

import (
    "bytes"
    "fmt"
    "os"
    "json"
    "http"
    "net"
    "io/ioutil"
)

var defaultHeaders = map[string]string{}

//
// Helper and utility functions (private)
//

// Converts given URL to string containing the body of the response.
func url_to_string(url string) []byte {
    if r, _, err := http.Get(url); err == nil {
        b, err := ioutil.ReadAll(r.Body)
        r.Body.Close()
        if err == nil {
            return b
        }
    }
    return make([]byte, 0)
}

type IdAndRev struct {
    Id  string "_id"
    Rev string "_rev"
}

// Simply extract id and rev from a given JSON string (typically a document)
func extract_id_and_rev(json_str []byte) (string, string, os.Error) {
    id_rev := new(IdAndRev)
    if err := json.Unmarshal(json_str, id_rev); err != nil {
        return "", "", err
    }
    return id_rev.Id, id_rev.Rev, nil
}

// Sends a query to CouchDB and parses the response back.
//
// method: the name of the HTTP method (POST, PUT,...)
// url: the URL to interact with
// headers: additional headers to pass to the request
// in: body of the request
// out: a structure to fill in with the returned JSON document
func (p Database) interact(method, url string, headers map[string]string, in []byte, out interface{}) (int, os.Error) {
    fullHeaders := map[string]string{}
    for k, v := range headers {
        fullHeaders[k] = v
    }

    bodyLength := 0
    if in != nil {
        bodyLength = len(in)
        fullHeaders["Content-Type"] = "application/json"
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
    req.URL, _ = http.ParseURL(url)
    if in != nil {
        req.Body = &buffer{bytes.NewBuffer(in)}
    }

    // Make connection
    conn, err := net.Dial("tcp", "", fmt.Sprintf("%s:%s", p.Host, p.Port))
    if err != nil {
        return 0, err
    }
    http_conn := http.NewClientConn(conn, nil)
    defer http_conn.Close()
    if err := http_conn.Write(&req); err != nil {
        return 0, err
    }

    // Read response
    r, err := http_conn.Read()
    if err != nil {
        return 0, err
    }
    if r.StatusCode < 200 || r.StatusCode >= 300 {
        b := []byte{}
        r.Body.Read(b)
        return r.StatusCode, os.NewError("server said: " + r.Status)
    }

    decoder := json.NewDecoder(r.Body)
    if err = decoder.Decode(out); err != nil {
        return 0, err
    }
    r.Body.Close()

    return r.StatusCode, nil
}

func (p Database) create_database() os.Error {
    ir := response{}
    if _, err := p.interact("PUT", p.DBURL(), defaultHeaders, nil, &ir); err != nil {
        return err
    }
    if !ir.Ok {
        return os.NewError(fmt.Sprintf("%s: %s", ir.Error, ir.Reason))
    }
    return nil
}

type buffer struct {
    b *bytes.Buffer
}

func (b *buffer) Read(out []byte) (int, os.Error) {
    return b.b.Read(out)
}

func (b *buffer) Close() os.Error { return nil }

//
// Database object + public methods
//

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
    url := fmt.Sprintf("%s/%s", p.BaseURL(), "_all_dbs")
    s := url_to_string(url)
    if len(s) > 0 {
        return true
    }
    return false
}

type DatabaseInfo struct {
    Db_name string
    // other stuff too, ignore for now
}

// Test whether specified database exists in specified CouchDB instance
func (p Database) Exists() bool {
    di := new(DatabaseInfo)
    if err := json.Unmarshal(url_to_string(p.DBURL()), di); err != nil {
        return false
    }
    if di.Db_name != p.Name {
        return false
    }
    return true
}

func NewDatabase(host, port, name string) (Database, os.Error) {
    db := Database{host, port, name}
    if !db.Running() {
        return db, os.NewError("CouchDB not running")
    }
    if !db.Exists() {
        if err := db.create_database(); err != nil {
            return db, err
        }
    }
    return db, nil
}

// Strip _id and _rev from d, returning them separately if they exist
func clean_JSON(d interface{}) (json_buf []byte, id, rev string, err os.Error) {
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
        m["_id"] = nil, false
    }
    if _, ok := m["_rev"]; ok {
        rev = id_rev.Rev
        m["_rev"] = nil, false
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
// Document may specify both _id and _rev (will overwrite existing)
//   or just _id (will use that id, but not overwrite existing)
//   or neither (will use autogenerated id)
func (p Database) Insert(d interface{}) (string, string, os.Error) {
    json_buf, id, rev, err := clean_JSON(d)
    if id != "" && rev != "" {
        new_rev, err2 := p.Edit(d)
        return id, new_rev, err2
    } else if id != "" {
        clean_d := map[string]interface{}{}
        if err = json.Unmarshal(json_buf, &clean_d); err != nil {
            return "", "", err
        }
        return p.InsertWith(clean_d, id)
    } else if id == "" {
        return p.insert(json_buf)
    }
    return "", "", os.NewError("invalid Document")
}

// Implementation of simple autogenerated-id insert
func (p Database) insert(json_buf []byte) (string, string, os.Error) {
    ir := response{}
    if _, err := p.interact("POST", p.DBURL(), defaultHeaders, json_buf, &ir); err != nil {
        return "", "", err
    }
    if !ir.Ok {
        return "", "", os.NewError(fmt.Sprintf("%s: %s", ir.Error, ir.Reason))
    }
    return ir.Id, ir.Rev, nil
}

// Inserts the given document (which should not specify _id or _rev fields)
// using the passed 'id' as the _id. Will fail if the id already exists.
func (p Database) InsertWith(d interface{}, id string) (string, string, os.Error) {
    json_buf, err := json.Marshal(d)
    if err != nil {
        return "", "", err
    }
    url := fmt.Sprintf("%s/%s", p.DBURL(), http.URLEscape(id))
    ir := response{}
    if _, err = p.interact("PUT", url, defaultHeaders, json_buf, &ir); err != nil {
        return "", "", err
    }
    if !ir.Ok {
        return "", "", os.NewError(fmt.Sprintf("%s: %s", ir.Error, ir.Reason))
    }
    return ir.Id, ir.Rev, nil
}

// Edits the given document, which must specify both _id and _rev fields, and
// returns the new revision.
func (p Database) Edit(d interface{}) (string, os.Error) {
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
        return "", os.NewError("Id not specified in interface")
    }
    if id_rev.Rev == "" {
        return "", os.NewError("Rev not specified in interface (try InsertWith)")
    }
    url := fmt.Sprintf("%s/%s", p.DBURL(), http.URLEscape(id_rev.Id))
    ir := response{}
    if _, err = p.interact("PUT", url, defaultHeaders, json_buf, &ir); err != nil {
        return "", err
    }
    return ir.Rev, nil
}

// Unmarshals the document matching id to the given interface, returning rev.
func (p Database) Retrieve(id string, d interface{}) (string, os.Error) {
    if id == "" {
        return "", os.NewError("no id specified")
    }
    json_str := url_to_string(fmt.Sprintf("%s/%s", p.DBURL(), id))
    retrieved_id, rev, err := extract_id_and_rev(json_str)
    if err != nil {
        return "", err
    }
    if retrieved_id != id {
        return "", os.NewError("invalid id specified")
    }
    return rev, json.Unmarshal(json_str, d)
}

// Deletes document given by id and rev.
func (p Database) Delete(id, rev string) os.Error {
    headers := map[string]string{
        "If-Match": rev,
    }
    url := fmt.Sprintf("%s/%s", p.DBURL(), id)

    ir := response{}
    if _, err := p.interact("DELETE", url, headers, nil, &ir); err != nil {
        return err
    }
    if !ir.Ok {
        return os.NewError(fmt.Sprintf("%s: %s", ir.Error, ir.Reason))
    }
    return nil
}

type Row struct {
    Id  string
    Key string
}

type keyed_view_response struct {
    Total_rows uint64
    Offset     uint64
    Rows       []Row
}

// Return array of document ids as returned by the given view/options combo.
// view should be eg. "_design/my_foo/_view/my_bar"
// options should be eg. { "limit": 10, "key": "baz" }
func (p Database) Query(view string, options map[string]interface{}) ([]string, os.Error) {
    if view == "" {
        return make([]string, 0), os.NewError("empty view")
    }

    parameters := ""
    for k, v := range options {
        switch t := v.(type) {
        case string:
            parameters += fmt.Sprintf(`%s="%s"&`, k, http.URLEscape(t))
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
    json_str := url_to_string(full_url)
    kvr := new(keyed_view_response)
    if err := json.Unmarshal(json_str, kvr); err != nil {
        return make([]string, 0), err
    }

    ids := make([]string, len(kvr.Rows))
    for i, row := range kvr.Rows {
        ids[i] = row.Id
    }
    return ids, nil
}
