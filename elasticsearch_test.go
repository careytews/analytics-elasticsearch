
package main

//
// Strategy here is to generate graph output from data in data.json and compare
// with output in expected.json.  Hopefully this won't be susceptible to
// randomness in the output order of keys in property maps?
//

import (
	"bytes"
	"testing"
	"encoding/json"
	"os"
	"io/ioutil"
	dt "github.com/trustnetworks/analytics-common/datatypes"
)

// Change this to true to get JSON suitable for the expected.json
// file to be sent to Stdout
const genExpectedJson bool = false

type Check func(*testing.T, dt.Bundle)

func aCheck(t *testing.T, elts dt.Bundle) {
	if elts["class"] != "uk.gov.gchq.gaffer.operation.impl.add.AddElements" {
		t.Errorf("BROKEN")
	}
}

func RunChecks(t *testing.T, e dt.Event, r Observation) {

	ob := Convert(&e)

	enc, err := json.Marshal(ob)
	if err != nil {
		t.Errorf("Couldn't marshal graph: %s",
			err.Error())
	}

	enc2, err := json.Marshal(r)
	if err != nil {
		t.Errorf("Couldn't marshal graph: %s",
			err.Error())
	}

	// You can use this bit of code to generate the content for expected.json file
	var out bytes.Buffer
	json.Indent(&out, enc, "", "  ")
	if genExpectedJson { out.WriteTo(os.Stdout) }
		
	if string(enc) != string(enc2) {
		t.Errorf("Expected doesn't match generated")
	}
}

func TestGraph(t *testing.T) {

	file := "data.json"
	f, err := os.Open(file)
	if err != nil {
		t.Errorf("Couldn't read %s: %s", file, err.Error())
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("Couldn't read file: %s", err.Error())
	}
	f.Close()

	var es []dt.Event
	err = json.Unmarshal(data, &es)
	if err != nil {
		t.Errorf("Couldn't unmarshal JSON from file: %s",
			err.Error())
	}

	file = "expected.json"
	f, err = os.Open(file)
	if err != nil {
		t.Errorf("Couldn't read %s: %s", file, err.Error())
	}
	data, err = ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("Couldn't read file: %s", err.Error())
	}
	f.Close()

	var rs []Observation
	err = json.Unmarshal(data, &rs)
	if err != nil {
		t.Errorf("Couldn't unmarshal JSON from file: %s",
			err.Error())
	}

	var first bool = true
	for i, e := range es {
		if first {
			if genExpectedJson { os.Stdout.WriteString("[\n")}
			first = false
		} else {
			if genExpectedJson { os.Stdout.WriteString(",\n")}
		}
		RunChecks(t, e, rs[i])
	}
	if genExpectedJson { os.Stdout.WriteString("\n]\n") }
	
}

