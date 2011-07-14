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
	db_r := DBRecord{Id: id, Rev: rev, Foo: 34, Bars: []string{"iota"}}
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

func TestQueryId(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	r1 := DBRecord{"test_queryid_001", "", 42, []string{"one", "two"}}
	id1, rev1, err := db.Insert(r1)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}
	r2 := DBRecord{"test_queryid_002", "", 43, []string{"three", "four"}}
	id2, rev2, err := db.Insert(r2)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}

	design := map[string]interface{}{}
	design["_id"] = "_design/testview"
	views := map[string]interface{}{}
	design["views"] = views
	v := map[string]string{}
	views["v"] = v
	v["map"] = "function(doc) { emit(null, doc._id); }"
	t.Logf("view 'map': %s", v["map"])

	designId, designRev, err := db.Insert(design)
	if err != nil {
		t.Fatalf("failed to insert design: %s", err)
	} else {
		t.Logf("designId %s, designRev %s", designId, designRev)
	}
	ids, err := db.QueryIds("_design/testview/_view/v", map[string]interface{}{})
	if err != nil {
		t.Fatalf("failed to query ids: %s", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, but got %d", len(ids))
	}
	if ids[0] != "test_queryid_001" {
		t.Fatalf("_id: expected %s, got %s", "test_queryid_001", ids[0])
	}
	if ids[1] != "test_queryid_002" {
		t.Fatalf("_id: expected %s, got %s", "test_queryid_002", ids[1])
	}
	err = db.Delete(id1, rev1)
	if err != nil {
		t.Fatalf("failed to delete record 1: %s", err)
	}
	err = db.Delete(id2, rev2)
	if err != nil {
		t.Fatalf("failed to delete record 2: %s", err)
	}
	err = db.Delete(designId, designRev)
	if err != nil {
		t.Fatalf("failed to delete design record: %s", err)
	}
}

type MyRow struct {
	Key   uint64
	Value uint64
}

type MyRows struct {
	Rows []MyRow
}

func TestQuery(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	r1 := DBRecord{"query_001", "", 42, []string{"one", "two"}}
	id1, rev1, err := db.Insert(r1)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}
	r2 := DBRecord{"query_002", "", 43, []string{"three", "four"}}
	id2, rev2, err := db.Insert(r2)
	if err != nil {
		t.Fatalf("failed to insert record: %s", err)
	}

	design := map[string]interface{}{}
	design["_id"] = "_design/testview"
	views := map[string]interface{}{}
	design["views"] = views
	v := map[string]string{}
	views["v"] = v
	v["map"] = "function(doc) { emit(1, doc.Foo); }"
	v["reduce"] = "function(key, values, rereduce) { return sum(values); }"

	designId, designRev, err := db.Insert(design)

	rows := MyRows{}
	err = db.Query("_design/testview/_view/v", map[string]interface{}{"group": true}, &rows)
	if err != nil {
		t.Fatalf("failed to query ids: %s", err)
	}
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, but got %d", len(rows.Rows))
	}
	if rows.Rows[0].Key != 1 {
		t.Fatalf("key: expected %d, got %s", 1, rows.Rows[0].Key)
	}
	if rows.Rows[0].Value != 85 {
		t.Fatalf("value: expected %d, got %d", 85, rows.Rows[0].Value)
	}

	err = db.Delete(id1, rev1)
	if err != nil {
		t.Fatalf("failed to delete record 1: %s", err)
	}
	err = db.Delete(id2, rev2)
	if err != nil {
		t.Fatalf("failed to delete record 2: %s", err)
	}
	err = db.Delete(designId, designRev)
	if err != nil {
		t.Fatalf("failed to delete design record: %s", err)
	}
}

type Issue10 struct {
	Id    string "_id"
	Rev   string "_rev"
	Name  string
	Email string
}

func TestIssue10(t *testing.T) {
	db, err := NewDatabase(TEST_HOST, TEST_PORT, TEST_NAME)
	if err != nil {
		t.Fatalf("error connecting to CouchDB: %s", err)
	}
	info := Issue10{"user_x", "", "x", "test@localhost"}
	t.Logf(" pre-Insert: %v\n", info)
	id, rev, err := db.Insert(info)
	t.Logf("post-Insert: %v\n", info)
	t.Logf("id = %s, rev = %s\n", id, rev)
	if err != nil {
		t.Fatalf("error inserting Issue10 record")
	}
	if id != info.Id {
		t.Fatalf("id: got %s, expected %s", id, info.Id)
	}
	if rev == "" {
		t.Fatalf("rev: got nothing, expected something")
	}
}

