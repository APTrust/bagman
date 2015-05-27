package main
import (
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_package builds a DPN bag from an APTrust object.
func main() {
	procUtil := workers.CreateProcUtil()
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.DPNPackageWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("dpn_package started")
	dpnConfig, err := dpn.LoadConfig("dpn/bagbuilder_config.json")
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	packager := dpn.NewPackager(procUtil, dpnConfig)
	consumer.AddHandler(packager)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
