package main

import (
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/client"
	"log"
	"os"
	"time"
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
		Id:          0,
		Name:        "sample_uva_bag.tar",
		Bucket:      "aptrust.receiving.virginia.edu",
		ETag:        "9876543210",
		BagDate:     bagDate,
		Institution: "University of Virginia",
		Date:        time.Now().UTC(),
		Note:        "Note for test entry",
		Action:      "Ingest",
		Stage:       "Record",
		Status:      bagman.StatusSuccess,
		Outcome:     bagman.StatusSuccess,
		Retry:       true,
		Reviewed:    false,
	}

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
