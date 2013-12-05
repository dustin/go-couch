package couch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

// Row represents a single row in a view response
type Row struct {
	ID  *string
	Key *string
}

type keyedViewResponse struct {
	TotalRows uint64 `json:"total_rows"`
	Offset    uint64
	Rows      []Row
}

// QueryIds returns a slice of document ids as returned by the given view/options combo.
// view should be eg. "_design/my_foo/_view/my_bar"
// options should be eg. { "limit": 10, "key": "baz" }
func (p Database) QueryIds(view string, options map[string]interface{}) ([]string, error) {
	kvr := keyedViewResponse{}

	if err := p.Query(view, options, &kvr); err != nil {
		return nil, err
	}

	var ids []string
	for _, row := range kvr.Rows {
		if row.ID != nil {
			ids = append(ids, *row.ID)
		}
	}
	return ids, nil
}

var errEmptyView = errors.New("empty view")

// DocID is a string type that isn't escaped in a view param
type DocID string

func qParam(k, v string) string {
	format := `"%s"`
	switch k {
	case "startkey_docid", "stale":
		format = "%s"
	}
	return fmt.Sprintf(format, v)
}

// ViewURL builds a URL for a view with the given ddoc, view name, and
// parameters.
func (p Database) ViewURL(view string, params map[string]interface{}) (string, error) {
	values := url.Values{}
	for k, v := range params {
		switch t := v.(type) {
		case DocID:
			values[k] = []string{string(t)}
		case string:
			values[k] = []string{qParam(k, t)}
		case int:
			values[k] = []string{fmt.Sprintf(`%d`, t)}
		case bool:
			values[k] = []string{fmt.Sprintf(`%v`, t)}
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("unsupported value-type %T in Query, "+
					"json encoder said %v", t, err)
			}
			values[k] = []string{fmt.Sprintf(`%v`, string(b))}
		}
	}

	u, err := url.Parse(p.DBURL() + "/" + view)
	must(err)
	u.RawQuery = values.Encode()

	return u.String(), nil
}

// Query executes and unmarshals a view request.
func (p Database) Query(view string, options map[string]interface{}, results interface{}) error {
	if view == "" {
		return errEmptyView
	}
	fullURL, err := p.ViewURL(view, options)
	if err != nil {
		return err
	}
	return unmarshalURL(fullURL, results)
}
