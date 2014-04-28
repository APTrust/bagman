package main

import (
	"fmt"
	"os"
	"github.com/APTrust/bagman/fluctus/client"
)

func main() {
	client, err := client.New("http://localhost:3000",
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_PASSWORD"))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	client.InitSession()
	// http://localhost:3000/catalog?utf8=%E2%9C%93&controller=institutions&action=show&search_field=all_fields&q=

}
