package main

import (
	"fmt"
	"os"
	"log"
	"time"
	"github.com/APTrust/bagman/fluctus/client"
)

func main() {
	logger := log.New(os.Stdout, "", 0)
	client, err := client.New("http://localhost:3000",
		"andrew.diamond@aptrust.org",
		"85a284ca0f69b50a4fe9f490733f2cebeb09d06f",
		logger)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	bag_date, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-05-01T12:00:00.000Z")
	client.GetBagStatus("SAMPLE_ETAG", "SAMPLE_NAME", bag_date)
	// http://localhost:3000/catalog?utf8=%E2%9C%93&controller=institutions&action=show&search_field=all_fields&q=

}
