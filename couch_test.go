// -*- tab-width: 4 -*-
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
    Id   string "_id"
    Rev  string "_rev"
    Foo  int64
    Bars []string
}

func TestInsert(t *testing.T) {
    db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
    if err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
    r := Record{12345, []string{"alpha", "beta", "delta"}}
    id, rev, err := db.Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    if err := db.Delete(id, rev); err != nil {
        t.Fatalf("failed to delete record: %s", err)
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
    if err := db.Delete(db_r.Id, db_r.Rev); err != nil {
        t.Fatalf("failed to delete record: %s", err)
    }
}

func TestEdit(t *testing.T) {
    db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
    if err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
    r := Record{10101, []string{"iota", "omicron", "nu"}}
    id, rev, err := db.Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    db_r := new(DBRecord)
    if _, err := db.Retrieve(id, db_r); err != nil {
        t.Fatalf("failed to retrieve record: %s", err)
    }
    db_r.Foo = 34
    new_rev, err := db.Edit(db_r)
    if err != nil {
        t.Fatalf("failed to edit record %s:%s: %s", id, rev, err)
    }
    r2 := new(Record)
    if _, err := db.Retrieve(id, r2); err != nil {
        t.Fatalf("failed to re-retrieve record: %s", err)
    }
    if r2.Foo != 34 {
        t.Fatalf("failed to save the change in Edit: Foo=%d, expected %d", r2.Foo, 34)
    }
    if err := db.Delete(db_r.Id, new_rev); err != nil {
        t.Fatalf("failed to delete record: %s", err)
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

func TestSpecificInsert(t *testing.T) {
    db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
    if err != nil {
        t.Fatalf("error connecting to CouchDB: %s", err)
    }
    r := DBRecord{"my_test_id", "", 42, []string{"one", "two"}}
    t.Logf("%v", r)
    id, rev, err := db.Insert(r)
    if err != nil {
        t.Fatalf("failed to insert record: %s", err)
    }
    if id != r.Id {
        t.Fatalf("specified id: expected %s, got %s", r.Id, id)
    }
    db_r := new(DBRecord)
    rev, err = db.Retrieve(id, db_r)
    if err != nil {
        t.Fatalf("failed to retrieve record: %s", err)
    }
    if id != db_r.Id {
        t.Errorf("id: expected %s, got %s", id, db_r.Id)
    }
    if rev != db_r.Rev {
        t.Errorf("rev: expected %s, got %s", rev, db_r.Rev)
    }
    db_r.Foo = 24
    rev, err = db.Edit(db_r)
    if err != nil {
        t.Errorf("failed to edit record: %s", err)
    }
    rev, err = db.Retrieve(id, db_r)
    if err != nil {
        t.Errorf("failed to re-retrieve record: %s", err)
    }
    if db_r.Foo != 24 {
        t.Errorf("after re-retreival, Foo: expected %d, got %d", 24, db_r.Foo)
    }
    err = db.Delete(db_r.Id, db_r.Rev)
    if err != nil {
        t.Fatalf("failed to delete record: %s", err)
    }
}
