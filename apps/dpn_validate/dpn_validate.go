package main

import (
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_validate validates bags copied from other nodes before we replicate them.
func main() {
	procUtil := workers.CreateProcUtil("dpn")
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.DPNValidationWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("dpn_validate started")
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json", procUtil.ConfigName)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	validator, err := dpn.NewValidator(procUtil, dpnConfig)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	consumer.AddHandler(validator)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
