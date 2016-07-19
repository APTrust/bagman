package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"io/ioutil"
	"os"
)

/*
apt_retry retries the record step of a failed item. This only works
on items that have passed the prepare and store steps, and then have
wound up in the trouble queue.
*/
func main() {
	jsonFile := flag.String("file", "", "JSON file to load")
	procUtil := workers.CreateProcUtil("aptrust")
	procUtil.MessageLog.Info("apt_retry started")
	bagRecorder := workers.NewBagRecorder(procUtil)

	if jsonFile == nil {
		fmt.Println("apt_retry tries to re-send metadata to Fluctus")
		fmt.Println("Usage: apt_retry -file=path/to/file.json -config=some_config")
		os.Exit(0)
	}
	fmt.Println(*jsonFile)
	result := loadJsonFile(*jsonFile)
	result.ErrorMessage = ""
	bagRecorder.RunWithoutNsq(result)
}

func loadJsonFile(jsonFile string) (*bagman.ProcessResult) {
	bytes, err := ioutil.ReadFile(jsonFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot read file '%s': %v\n", jsonFile, err)
		os.Exit(1)
	}
	var result bagman.ProcessResult
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot convert JSON to object: %v\n", err)
		os.Exit(1)
	}
	return &result
}
