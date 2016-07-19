package main
import (
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_package builds a DPN bag from an APTrust object.
func main() {
	procUtil := workers.CreateProcUtil("dpn")
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.DPNTroubleWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("dpn_trouble started")
	troubleWorker := dpn.NewTroubleProcessor(procUtil)
	consumer.AddHandler(troubleWorker)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
