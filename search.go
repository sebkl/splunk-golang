package splunk

import (
        "fmt"
        "net/url"
	"encoding/json"
	"bytes"
	"strings"
)

type Value interface{}

type Row struct {
	Preview bool `json:"preview"`
	Offset int `json:"offset"`
	Result map[string]Value `json:"result"`
}

type Rows []Row

func NewRow() (ret Row) {
	ret = Row{}
	ret.Result = make(map[string]Value)
	return
}

func (conn SplunkConnection) Search(searchString string) (rows Rows,err error) {
	data := make(url.Values)
	data.Add("search",searchString)
	data.Add("output_mode","json")

	/* TODO: return stream in order to read reponses that do not fit in memory. */
	response, err := conn.httpPost(fmt.Sprintf("%s/servicesNS/admin/search/search/jobs/export",conn.BaseURL),&data)

	if err != nil {
		return nil,err
	}

	lines := strings.Split(response,"\n")
	rows = make(Rows,len(lines))
	var ni int
	ni = 0

	for _,v := range lines {
		fbuf := bytes.NewBufferString(v)
		dec := json.NewDecoder(fbuf)
		r := NewRow()
		if  err = dec.Decode(&r); err != nil {
			fmt.Printf("Could not decode line: '%s' %s\n",v,err)
		} else {
			rows[ni] = r
			ni++
		}
	}
	return rows[:ni], nil;
}
