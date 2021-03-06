package main

import (
	"github.com/APTrust/bagman/workers"
)

func main() {
	procUtil := workers.CreateProcUtil("aptrust")
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.FixityWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_fixity started")
	fixityChecker := workers.NewFixityChecker(procUtil)
	consumer.AddHandler(fixityChecker)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
