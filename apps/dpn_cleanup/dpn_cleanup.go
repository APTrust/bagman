package main
import (
	"github.com/APTrust/bagman/dpn"
	"github.com/APTrust/bagman/workers"
)

// dpn_cleanup deletes files we no longer need from the staging
// area, such as tar files that have been replicated to other nodes.
//
// This is meant to be run as a cron job, and does not need to connect
// to NSQ.
//
// A typical cron entry for this to run hourly might look like this:
//
// 0 * * * * . $HOME/.bash_profile /home/ubuntu/go/src/github.com/APTrust/bagman/bin/dpn_cleanup -config=demo
func main() {
	procUtil := workers.CreateProcUtil("dpn")
	procUtil.MessageLog.Info("dpn_cleanup started")
	dpnConfig, err := dpn.LoadConfig("dpn/dpn_config.json", procUtil.ConfigName)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	cleanup, err := dpn.NewCleanup(procUtil, dpnConfig)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	cleanup.DeleteReplicatedBags()
}
