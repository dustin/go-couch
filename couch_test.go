package couch

import (
    "testing"
    "reflect"
)

const (
    TEST_HOST = "127.0.0.1"
    TEST_PORT = "5984"
    TEST_NAME = "couch-go-testdb"
)

func TestConnectivity(t *testing.T) {
    if _, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME); err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
}

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

func TestInsert(t *testing.T) {
    db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
    if err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
    r := Record{12345, []string{"alpha", "beta", "delta"}}
    if _, _, err := db.Insert(r); err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
}

func TestRetrieve(t *testing.T) {
    db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
    if err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
    r := Record{999, []string{"kappa", "gamma"}}
    id, _, err := db.Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    db_r := new(DBRecord)
    rev, err := db.Retrieve(id, db_r)
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
    db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
    if err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
    r := Record{10101, []string{"iota", "omicron", "nu"}}
    id, _, err := db.Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    db_r := new(DBRecord)
    if _, err := db.Retrieve(id, db_r); err != nil {
        t.Fatalf("failed to retrieve record: %s", err)
    }
    db_r.Foo = 1
    if _, err := db.Edit(db_r); err != nil {
        t.Fatalf("failed to edit record: %s", err)
    }
}

func TestDelete(t *testing.T) {
    db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
    if err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
    r := Record{321, []string{"zeta", "phi"}}
    id, rev, err := db.Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    if err := db.Delete(id, rev); err != nil {
        t.Fatalf("failed to delete record: %s", err)
    }
}
