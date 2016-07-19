package main
import (
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_copy copies bags from remote nodes so we can replicate
// those bags. The copy is done via rsync over ssh.
func main() {
	procUtil := workers.CreateProcUtil("dpn")
	procUtil.MessageLog.Info("Connecting to NSQLookupd at %s", procUtil.Config.NsqLookupd)
	procUtil.MessageLog.Info("NSQDHttpAddress is %s", procUtil.Config.NsqdHttpAddress)
	consumer, err := workers.CreateNsqConsumer(&procUtil.Config, &procUtil.Config.DPNCopyWorker)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	procUtil.MessageLog.Info("dpn_copy started")
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json", procUtil.ConfigName)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	copier, err := dpn.NewCopier(procUtil, dpnConfig)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	consumer.AddHandler(copier)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	procUtil.MessageLog.Info("**** If the NSQ lookup service returns a " +
		"hostname that is not a fully-qualified domain name, be sure " +
		"that name is in this system's /etc/hosts file! ****")

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan

}
