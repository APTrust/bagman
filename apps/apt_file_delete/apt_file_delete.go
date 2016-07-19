package main

import (
	"github.com/APTrust/bagman/workers"
)

// apt_file_delete - Deletes individual Generic Files from preservation
// storage at the request of users/admins.
func main() {
	procUtil := workers.CreateProcUtil("aptrust")
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.FileDeleteWorker)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}
	procUtil.MessageLog.Info("apt_file_delete started")
	fileDeleter := workers.NewFileDeleter(procUtil)
	consumer.AddHandler(fileDeleter)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}
