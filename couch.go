// -*- tab-width: 4 -*-
package couch

import (
	"bytes"
    "strings"
    "fmt"
    "os"
    "json"
    "http"
    "net"
    "io/ioutil"
)

//
// Helper and utility functions (private)
//

// Replaces all instances of from with to in s (quite inefficient right now)
func replace(s, from, to string) string {
    toks := strings.SplitAfter(s, from, -1)
    newstr := ""
    for i, tok := range toks {
        if i < len(toks)-1 {
            if !strings.HasSuffix(tok, from) {
                panic("problem in replace")
            }
            newtok := tok[0 : len(tok)-len(from)]
            newstr = newstr + newtok + to
        } else {
            newstr = newstr + tok
        }
    }
    return newstr
}

// Converts given URL to string containing the body of the response.
func url_to_string(url string) string {
    if r, _, err := http.Get(url); err == nil {
        b, err := ioutil.ReadAll(r.Body)
        r.Body.Close()
        if err == nil {
            return string(b)
        }
    }
    return ""
}

// Marshal given interface to JSON string
func to_JSON(p interface{}) (result string, err os.Error) {
    err = nil
    result = ""
    if buf, err := json.Marshal(p); err == nil {
        result = string(buf)
    }
    return
}

// Unmarshal JSON string to given interface
func from_JSON(s string, p interface{}) (err os.Error) {
    err = json.Unmarshal([]byte(s), p)
    return
}

type IdAndRev struct {
    Id  string "_id"
    Rev string "_rev"
}

// Simply extract id and rev from a given JSON string (typically a document)
func extract_id_and_rev(json_str string) (string, string, os.Error) {
    id_rev := new(IdAndRev)
    if err := from_JSON(json_str, id_rev); err != nil {
        return "", "", err
    }
    return id_rev.Id, id_rev.Rev, nil
}

type CreateResponse struct {
    Ok bool
}

func (p Database) create_database() os.Error {
    // Set up request
    var req http.Request
    req.Method = "PUT"
    req.ProtoMajor = 1
    req.ProtoMinor = 1
    req.Close = true
    req.Header = map[string]string{
        "Content-Type": "application/json",
    }
    req.TransferEncoding = []string{"chunked"}
    req.URL, _ = http.ParseURL(p.DBURL())

    // Make connection
    conn, err := net.Dial("tcp", "", fmt.Sprintf("%s:%s", p.Host, p.Port))
    if err != nil {
        return err
    }
    http_conn := http.NewClientConn(conn, nil)
    defer http_conn.Close()
    if err := http_conn.Write(&req); err != nil {
        return err
    }

    // Read response
    r, err := http_conn.Read()
    if r == nil {
        return os.NewError("no response")
    }
    if err != nil {
        return err
    }
    data, _ := ioutil.ReadAll(r.Body)
    r.Body.Close()
    cr := new(CreateResponse)
    if err := from_JSON(string(data), cr); err != nil {
        return err
    }
    if !cr.Ok {
        return os.NewError("CouchDB returned not-OK")
    }

    return nil
}

type stringCloser struct {
    s   string
    pos int
}

func (sc stringCloser) Close() os.Error { return nil }

func (sc stringCloser) Read(p []byte) (int, os.Error) {
    i := 0
    for i = 0; i < len(p) && i < len(sc.s); i++ {
        p[i] = sc.s[i]
        sc.pos++
    }
    if sc.pos == len(sc.s) {
        return i, os.EOF
    }
    return i, nil
}

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
    if err := from_JSON(url_to_string(p.DBURL()), di); err != nil {
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

type InsertResponse struct {
    Ok  bool
    Id  string
    Rev string
	Error string
	Reason string
}

// Inserts document to CouchDB, returning id and rev on success.
func (p Database) Insert(d interface{}, id *string) (string, string, os.Error) {
	json_buf, err := json.Marshal(d)
    if err != nil {
        return "", "", err
    }
    // use the dict representation of a JSON document to
    // manipulate the _id and _ref fields
    tmp := map[string]interface{}{}
    err = json.Unmarshal(json_buf, &tmp)
    if err != nil {
        return "", "", err
    }
	tmp["_rev"] = nil, false
    if id != nil {
        tmp["_id"] = id
    } else {
		tmp["_id"] = nil, false
	}
    json_buf, err = json.Marshal(tmp)
    if err != nil {
        return "", "", err
    }
    r, err := http.Post(p.DBURL(), "application/json", bytes.NewBuffer(json_buf))
    if err != nil {
        return "", "", err
    }
    b, err := ioutil.ReadAll(r.Body)
    r.Body.Close()
    if err != nil {
        return "", "", err
    }
    ir := new(InsertResponse)
    if err := from_JSON(string(b), ir); err != nil {
        return "", "", err
    }
    if !ir.Ok {
        return "", "", os.NewError(ir.Error + ": " + ir.Reason)
    }
    return ir.Id, ir.Rev, nil
}

// Edits the given document, which must specify both Id and Rev fields, and
// returns the new revision.
func (p Database) Edit(d interface{}) (string, os.Error) {
    json_str, err := to_JSON(d)
    if err != nil {
        return "", err
    }
    id_rev := new(IdAndRev)
    err = from_JSON(json_str, id_rev)
    if err != nil {
        return "", err
    }
    if len(id_rev.Id) <= 0 {
        return "", os.NewError("Id not specified in interface")
    }
    // Set up request
    var req http.Request
    req.Method = "PUT"
    req.ProtoMajor = 1
    req.ProtoMinor = 1
    req.Close = true
    req.Header = map[string]string{
        "Content-Type": "application/json",
    }
    req.TransferEncoding = []string{"chunked"}
    req.URL, _ = http.ParseURL(fmt.Sprintf("%s/%s", p.DBURL(), id_rev.Id))
    req.Body = stringCloser{json_str, 0}

    // Make connection
    conn, err := net.Dial("tcp", "", fmt.Sprintf("%s:%s", p.Host, p.Port))
    if err != nil {
        return "", err
    }
    http_conn := http.NewClientConn(conn, nil)
    defer http_conn.Close()
    if err := http_conn.Write(&req); err != nil {
        return "", err
    }
    // Read response
    r, err := http_conn.Read()
    if r == nil {
        return "", os.NewError("no response")
    }
    if err != nil {
        return "", err
    }
    data, _ := ioutil.ReadAll(r.Body)
    r.Body.Close()
    ir := new(InsertResponse)
    if err := from_JSON(string(data), ir); err != nil {
        return "", err
    }
    if !ir.Ok {
        return "", os.NewError("CouchDB returned not-OK")
    }
    return ir.Rev, nil
}

type RetrieveError struct {
    Error  string
    Reason string
}

// Unmarshals the document matching id to the given interface, returning rev.
func (p Database) Retrieve(id string, d interface{}) (string, os.Error) {
    if len(id) <= 0 {
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
    return rev, from_JSON(json_str, d)
}

// Deletes document given by id and rev.
func (p Database) Delete(id, rev string) os.Error {
    // Set up request
    var req http.Request
    req.Method = "DELETE"
    req.ProtoMajor = 1
    req.ProtoMinor = 1
    req.Close = true
    req.Header = map[string]string{
        "Content-Type": "application/json",
        "If-Match":     rev,
    }
    req.TransferEncoding = []string{"chunked"}
    req.URL, _ = http.ParseURL(fmt.Sprintf("%s/%s", p.DBURL(), id))

    // Make connection
    conn, err := net.Dial("tcp", "", fmt.Sprintf("%s:%s", p.Host, p.Port))
    if err != nil {
        return err
    }
    http_conn := http.NewClientConn(conn, nil)
    defer http_conn.Close()
    if err := http_conn.Write(&req); err != nil {
        return err
    }

    // Read response
    r, err := http_conn.Read()
    if r == nil {
        return os.NewError("no response")
    }
    if err != nil {
        return err
    }
    data, _ := ioutil.ReadAll(r.Body)
    r.Body.Close()
    ir := new(InsertResponse)
    if err := from_JSON(string(data), ir); err != nil {
        return err
    }
    if !ir.Ok {
        return os.NewError("CouchDB returned not-OK")
    }

    return nil
}

type Row struct {
    Id  string
    Key string
}

type KeyedViewResponse struct {
    Total_rows uint64
    Offset     uint64
    Rows       []Row
}

// Return array of document ids as returned by the given view/options combo.
// view should be eg. "_design/my_foo/_view/my_bar"
// options should be eg. { "limit": 10, "key": "baz" }
func (p Database) Query(view string, options map[string]interface{}) ([]string, os.Error) {
    if len(view) <= 0 {
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
    kvr := new(KeyedViewResponse)
    if err := from_JSON(json_str, kvr); err != nil {
        return make([]string, 0), err
    }

    ids := make([]string, len(kvr.Rows))
    for i, row := range kvr.Rows {
        ids[i] = row.Id
    }
    return ids, nil
}
