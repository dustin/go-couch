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

func (p Database) Query(view string, options map[string]interface{}, results interface{}) error {
	if view == "" {
		return errEmptyView
	}
	parameters := ""
	for k, v := range options {
		switch t := v.(type) {
		case string:
			parameters += fmt.Sprintf(`%s="%s"&`, k, url.QueryEscape(t))
		case int:
			parameters += fmt.Sprintf(`%s=%d&`, k, t)
		case bool:
			parameters += fmt.Sprintf(`%s=%v&`, k, t)
		default:
			b, err := json.Marshal(v)
			if err != nil {
				panic(fmt.Sprintf("unsupported value-type %T in Query, json encoder said %v", t, err))
			}
			parameters += fmt.Sprintf(`%s=%v&`, k, string(b))
		}
	}
	full_url := fmt.Sprintf("%s/%s?%s", p.DBURL(), view, parameters)

	return unmarshalURL(full_url, results)
}
