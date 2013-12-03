package couch

import (
	"testing"
)

func TestQueryNoView(t *testing.T) {
	d := Database{}
	err := d.Query("", nil, nil)
	if err != errEmptyView {
		t.Errorf("Expected empty view error, got %v", err)
	}
}

func TestQueryIdsNoView(t *testing.T) {
	d := Database{}
	x, err := d.QueryIds("", nil)
	if err != errEmptyView {
		t.Errorf("Expected empty view error, got %v/%v", x, err)
	}
}
