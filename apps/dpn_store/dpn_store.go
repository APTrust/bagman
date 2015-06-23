package main

import (
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_store copies DPN bags from our local staging area to long-term storage
// on AWS S3.
func main() {
	procUtil := workers.CreateProcUtil()
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.DPNStoreWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("dpn_store started")
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json")
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	storer, err := dpn.NewStorer(procUtil, dpnConfig)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	consumer.AddHandler(storer)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
