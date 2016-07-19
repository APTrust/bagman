package main

import (
	"github.com/APTrust/bagman/workers"
)

// apt_trouble dumps information about bags that can't be ingested
// into simple JSON files.
func main() {
	procUtil := workers.CreateProcUtil("aptrust")
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.TroubleWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_trouble started")
	troubleProcessor := workers.NewTroubleProcessor(procUtil)
	consumer.AddHandler(troubleProcessor)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
