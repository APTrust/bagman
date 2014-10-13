package main

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"os"
)

func main() {
	path := os.Args[1]
	if path == "" {
		fmt.Println("readtest.go: Reads a bag and outputs results.")
		fmt.Println("Usage: go run readtest.go /path/to/bag")
	} else {
		result := bagman.ReadBag(path)
		json, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(json))
	}
}
