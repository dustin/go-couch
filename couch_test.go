package couch

import (
    "testing"
    "reflect"
)

type Record struct {
    Foo  int64
    Bars []string
}

type DBRecord struct {
    Id   string
    Rev  string
    Foo  int64
    Bars []string
}


// IMPORTANT! These must be set correctly for the tests to succeed.
const (
    TestDBHost = "localhost"
    TestDBPort = "5984"
    TestDBName = "testdb"
)

func init() {
    CouchDBHost = TestDBHost
    CouchDBPort = TestDBPort
    CouchDBName = TestDBName
}

type DatabaseInfo struct {
    Db_name string
}

func TestConnectivity(t *testing.T) {
    di := new(DatabaseInfo)
    if _, err := Retrieve("/", di); err != nil {
        t.Fatalf("error contacting %s DB (is CouchDB running?)", CouchDBName, err)
    } 
    if di.Db_name != CouchDBName {
        t.Fatalf("error connecting to %s DB (did you create it?)", CouchDBName) 
    }
}

func TestInsert(t *testing.T) {
    r := Record{ 12345, []string{"alpha", "beta", "delta"} }
    _, _, err := Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
}

func TestRetrieve(t *testing.T) {
    r := Record{ 999, []string{"kappa", "gamma"} }
    id, _, err := Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    db_r := new(DBRecord)
    rev, err := Retrieve(id, db_r)
    if err != nil {
        t.Fatalf("failed to retrieve record: %s", err)
    }
    if id != db_r.Id {
        t.Fatalf("id: expected %s, got %s", id, db_r.Id)
    }
    if rev != db_r.Rev {
        t.Fatalf("rev: expected %s, got %s", rev, db_r.Rev)
    }
    if db_r.Foo != r.Foo {
        t.Fatalf("foo: expected %d, got %d", r.Foo, db_r.Foo)
    }
    if !reflect.DeepEqual(db_r.Bars, r.Bars) {
        t.Fatalf("bars: expected %v, got %v", r.Bars, db_r.Bars)
    }
}

func TestEdit(t *testing.T) {
    r := Record{ 10101, []string{"iota", "omicron", "nu"} }
    id, _, err := Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    db_r := new(DBRecord)
    if _, err := Retrieve(id, db_r); err != nil {
        t.Fatalf("failed to retrieve record: %s", err)
    }
    db_r.Foo = 1
    if _, err := Edit(db_r); err != nil {
        t.Fatalf("failed to edit record: %s", err)
    }
}

func TestDelete(t *testing.T) {
    r := Record{ 321, []string{"zeta", "phi"} }
    id, rev, err := Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    if err := Delete(id, rev); err != nil {
        t.Fatalf("failed to delete record: %s", err)
    }
}
