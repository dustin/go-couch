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

func TestDeleteDatabase(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	err = db.DeleteDatabase()
	if err != nil {
		t.Fatalf("error deleting database %s: %s", TEST_NAME, err)
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

func TestManualEdit(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	r := Record{10101, []string{"iota", "omicron", "nu"}}
	id, rev, err := db.Insert(r)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}
	db_r := DBRecord{Id:id, Rev:rev, Foo:34, Bars:[]string{"iota"}}
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

func TestInsertId(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	r := DBRecord{"my_test_id", "", 42, []string{"one", "two"}}
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
		t.Fatalf("id: expected %s, got %s", id, db_r.Id)
	}
	if rev != db_r.Rev {
		t.Fatalf("rev: expected %s, got %s", rev, db_r.Rev)
	}
	db_r.Foo = 24
	rev, err = db.Edit(db_r)
	if err != nil {
		t.Fatalf("failed to edit record: %s", err)
	}
	rev, err = db.Retrieve(id, db_r)
	if err != nil {
		t.Fatalf("failed to re-retrieve record: %s", err)
	}
	if db_r.Foo != 24 {
		t.Fatalf("after re-retreival, Foo: expected %d, got %d", 24, db_r.Foo)
	}
	err = db.Delete(db_r.Id, db_r.Rev)
	if err != nil {
		t.Fatalf("failed to delete record: %s", err)
	}
}

func TestInsertWith(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	r := Record{101, []string{"iota", "yotta"}}
	my_id := "test_id_101"
	id, rev, err := db.InsertWith(r, my_id)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}
	if id != my_id {
		t.Fatalf("with id: expected %s, got %s", my_id, id)
	}
	db_r := new(DBRecord)
	rev, err = db.Retrieve(my_id, db_r)
	if err != nil {
		t.Fatalf("failed to retrieve record: %s", err)
	}
	if my_id != db_r.Id {
		t.Fatalf("id: expected %s, got %s", my_id, db_r.Id)
	}
	if rev != db_r.Rev {
		t.Fatalf("rev: expected %s, got %s", rev, db_r.Rev)
	}
	err = db.Delete(db_r.Id, db_r.Rev)
	if err != nil {
		t.Fatalf("failed to delete record: %s", err)
	}
}

func TestInsertAsEdit(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	r := Record{101, []string{"iota", "yotta"}}
	my_id := "test_id_202"
	id, rev, err := db.InsertWith(r, my_id)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}
	if id != my_id {
		t.Fatalf("with id: expected %s, got %s", my_id, id)
	}
	db_r := new(DBRecord)
	rev, err = db.Retrieve(my_id, db_r)
	if err != nil {
		t.Fatalf("failed to retrieve record: %s", err)
	}
	if my_id != db_r.Id {
		t.Fatalf("id: expected %s, got %s", my_id, db_r.Id)
	}
	if rev != db_r.Rev {
		t.Fatalf("rev: expected %s, got %s", rev, db_r.Rev)
	}
	id, rev, err = db.Insert(db_r)
	if err != nil {
		t.Fatalf("failed to insert-as-edit: %s", err)
	}
	if id != my_id {
		t.Fatalf("id: expected %s, got %s", my_id, id)
	}
	err = db.Delete(db_r.Id, rev)
	if err != nil {
		t.Fatalf("failed to delete record: %s", err)
	}
}

func TestEditWith(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	r := Record{7, []string{"x"}}
	id, rev, err := db.Insert(r)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}
	r.Foo = 14
	new_rev, err2 := db.EditWith(r, id, rev)
	if err2 != nil {
		t.Fatalf("failed to EditWith: %s", err)
	}
	r.Foo = 28
	newest_rev, err3 := db.EditWith(r, id, new_rev)
	if err3 != nil {
		t.Fatalf("failed second EditWith: %s", err)
	}
	db_r := new(DBRecord)
	rev, err = db.Retrieve(id, db_r)
	if err != nil {
		t.Fatalf("failed to retrieve record: %s", err)
	}
	if db_r.Foo != 28 {
		t.Fatalf("Foo: expected %d, got %d", 28, db_r.Foo)
	}
	if db_r.Rev != newest_rev {
		t.Fatalf("rev: expected %s, got %s", newest_rev, db_r.Rev)
	}
	err = db.Delete(db_r.Id, db_r.Rev)
	if err != nil {
		t.Fatalf("failed to delete record: %s", err)
	}
}
