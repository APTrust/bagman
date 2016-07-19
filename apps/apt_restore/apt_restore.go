package main
import (
	"github.com/APTrust/bagman/workers"
)

// apt_restore restores bags from preservation storage into an
// institution's restore bucket.
func main() {
	procUtil := workers.CreateProcUtil("aptrust")
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.RestoreWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("apt_restore started")
	bagRestorer := workers.NewBagRestorer(procUtil)
	consumer.AddHandler(bagRestorer)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
