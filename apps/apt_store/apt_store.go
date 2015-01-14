package main

import (
	"github.com/APTrust/bagman/workers"
)

// apt_store stores bags that have been unpacked and validated
// by apt_prepare.
func main() {
	procUtil := workers.CreateProcUtil()
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.StoreWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_store started")
	bagStorer := workers.NewBagStorer(procUtil)
	consumer.AddHandler(bagStorer)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
