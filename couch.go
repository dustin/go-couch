package couch

import (
    "strings"
    "fmt"
    "os"
    "json"
    "bytes"
    "http"
    "io/ioutil"
)

var (
    CouchDBHost = "localhost"
    CouchDBPort = "5984"
    CouchDBName = "exampledb"
    CouchDBURL  = ""
)

func init() {
    CouchDBURL = fmt.Sprintf("http://%s:%s/%s/", CouchDBHost, CouchDBPort, CouchDBName)
}

//
// Helper and utility functions (private)
//

// Replaces all instances of from with to in s (quite inefficient right now)
func replace(s, from, to string) string {
    toks := strings.SplitAfter(s, from, 0)
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
func to_JSON(p interface{}) (string, os.Error) {
    buf := new(bytes.Buffer)
    if err := json.Marshal(buf, p); err != nil {
        return "", err
    }
    return buf.String(), nil
}

// Unmarshal JSON string to given interface
func from_JSON(s string, p interface{}) os.Error {
    if ok, errtok := json.Unmarshal(s, p); !ok {
        return os.NewError(fmt.Sprintf("error unmarshaling: %s", errtok))
    }
    return nil
}

// Since the json pkg doesn't handle fields beginning with _, we need to
// convert "_id" and "_rev" to "Id" and "Rev" to extract that data.
func temp_hack_json_id_rev(json_str string) string {
    json_str = replace(json_str, `"Id"`, `"_id"`)
    json_str = replace(json_str, `"Rev"`, `"_rev"`)
    return json_str
}

type IdAndRev struct {
    Id  string
    Rev string
}

// Simply extract id and rev from a given JSON string (typically a document)
func extract_id_and_rev(json_str string) (string, string, os.Error) {
    // this assumes the temp replacement hack has already been applied
    id_rev := new(IdAndRev)
    if err := from_JSON(json_str, id_rev); err != nil {
        return "", "", err
    }
    return id_rev.Id, id_rev.Rev, nil
}


//
// Interface functions (public)
//


type InsertResponse struct {
    Ok  bool
    Id  string
    Rev string
}

// Inserts document to CouchDB, returning id and rev on success. The document
// interface may optionally specify an "Id" field.
func Insert(p interface{}) (string, string, os.Error) {
    body_type := "application/json"
    json_str, err := to_JSON(p)
    if err != nil {
        return "", "", err
    }
    json_str = temp_hack_json_id_rev(json_str)

    r, err := http.Post(CouchDBURL, body_type, bytes.NewBufferString(json_str))
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
        return "", "", os.NewError("CouchDB returned not-OK")
    }

    return ir.Id, ir.Rev, nil
}

// Unmarshals the document matching id to the given interface, returning rev.
func Retrieve(id string, p interface{}) (string, os.Error) {
    if len(id) <= 0 {
        return "", os.NewError("no id specified")
    }

    json_str := url_to_string(fmt.Sprintf("%s%s", CouchDBURL, id))
    json_str = temp_hack_json_id_rev(json_str)
    _, rev, err := extract_id_and_rev(json_str)
    if err != nil {
        return "", err
    }

    return rev, from_JSON(json_str, p)
}

// Edits the given document, which must specify both id and rev fields (as "Id"
// and "Rev"), and returns the new rev.
func Edit(p interface{}) (string, os.Error) {
    _, rev, err := Insert(p)
    return rev, err
}

//
func Delete(id, rev string) os.Error {
    return os.NewError("delete not yet implemented")
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

// Return array of document ids as returned by the given view, by given key.
func RetrieveIds(view, key string) []string {
    // view should be eg. "_design/my_foo/_view/my_bar"
    if len(view) <= 0 || len(key) <= 0 {
        return make([]string, 0)
    }
    
    full_url := fmt.Sprintf(`%s%s?key="%s"`, CouchDBURL, view, key)
    json_str := url_to_string(full_url)
    kvr := new(KeyedViewResponse)
    if err := from_JSON(json_str, kvr); err != nil {
        return make([]string, 0)
    }
    
    ids := make([]string, len(kvr.Rows))
    for i, row := range kvr.Rows {
        ids[i] = row.Id
    }
    return ids    
}
