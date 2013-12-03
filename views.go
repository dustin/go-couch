package couch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

type keyedViewResponse struct {
	Total_rows uint64
	Offset     uint64
	Rows       []Row
}

// Return array of document ids as returned by the given view/options combo.
// view should be eg. "_design/my_foo/_view/my_bar"
// options should be eg. { "limit": 10, "key": "baz" }
func (p Database) QueryIds(view string, options map[string]interface{}) ([]string, error) {
	kvr := keyedViewResponse{}

	if err := p.Query(view, options, &kvr); err != nil {
		return nil, err
	}

	var ids []string
	for _, row := range kvr.Rows {
		if row.Id != nil {
			ids = append(ids, *row.Id)
		}
	}
	return ids, nil
}

var errEmptyView = errors.New("empty view")

// DocId is a string type that isn't escaped in a view param
type DocId string

func qParam(k, v string) string {
	format := `"%s"`
	switch k {
	case "startkey_docid", "stale":
		format = "%s"
	}
	return fmt.Sprintf(format, v)
}

// Build a URL for a view with the given ddoc, view name, and
// parameters.
func (p Database) ViewURL(view string, params map[string]interface{}) (string, error) {
	values := url.Values{}
	for k, v := range params {
		switch t := v.(type) {
		case DocId:
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

func (p Database) Query(view string, options map[string]interface{}, results interface{}) error {
	if view == "" {
		return errEmptyView
	}
	fullUrl, err := p.ViewURL(view, options)
	if err != nil {
		return err
	}
	return unmarshalURL(fullUrl, results)
}
