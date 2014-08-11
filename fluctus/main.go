package main

import (
	"fmt"
	"os"
	"log"
	"time"
	"github.com/APTrust/bagman/fluctus/client"
	"github.com/APTrust/bagman"
)

// TODO: Move this into client_test
func main() {
	logger := log.New(os.Stdout, "", 0)
	client, err := client.New(
		"http://localhost:3000",
		"v1",
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		logger)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	bagDate, _ := time.Parse("2006-01-02T15:04:05.000Z", "2014-05-01T12:00:00.000Z")
	status := &bagman.ProcessStatus{
		0,
		"sample_uva_bag.tar",
		"aptrust.receiving.virginia.edu",
		"9876543210",
		bagDate,
		"University of Virginia",
		time.Now().UTC(),
		"Note for test entry",
		"Ingest",
		"Record",
		bagman.StatusSuccess,
		bagman.StatusSuccess}


	remoteStatus, err := client.GetBagStatus(status.ETag, status.Name, status.BagDate)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Remote status is", remoteStatus)
	if remoteStatus != nil {
		status = remoteStatus
	}
	status.Date = time.Now().UTC()
	status.Action = bagman.ActionIngest
	status.Status = bagman.StatusSuccess
	status.Outcome = string(bagman.StatusSuccess)
	err = client.UpdateBagStatus(status)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	} else {
		fmt.Println("Bag status update succeeded")
	}

	// http://localhost:3000/catalog?utf8=%E2%9C%93&controller=institutions&action=show&search_field=all_fields&q=

}
