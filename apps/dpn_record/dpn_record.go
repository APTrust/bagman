package main

import (
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_record records the results of operations on DPN bags in
// both the local DPN REST service and in APTrust's Fluctus
// (Fedora) service. For example, when we replicate a bag from
// another DPN node, the recording service records the result
// in the DPN REST service of the remote node that originally
// requested the replication. When we ingest a bag from APTrust
// into DPN, the service creates the new bag entry and replication
// requests in our local DPN registry, and it created PREMIS events
// in Fluctus (Fedora) so that APTrust has a record of the bag
// being copied to DPN.
func main() {
	procUtil := workers.CreateProcUtil()
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.DPNRecordWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("dpn_record started")
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json", procUtil.ConfigName)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	recorder, err := dpn.NewRecorder(procUtil, dpnConfig)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	consumer.AddHandler(recorder)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	procUtil.MessageLog.Info("**** If the NSQ lookup service returns a " +
		"hostname that is not a fully-qualified domain name, be sure " +
		"that name is in this system's /etc/hosts file! ****")

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
