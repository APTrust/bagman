package main

import (
	"github.com/APTrust/bagman/workers"
)

// apt_failed_fixity dumps information about fixity checks that could
// not be completed into simple JSON files.
func main() {
	procUtil := workers.CreateProcUtil()
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.FailedFixityWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_failed_fixity started")
	processor := workers.NewFailedFixityProcessor(procUtil)
	consumer.SetHandler(processor)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
