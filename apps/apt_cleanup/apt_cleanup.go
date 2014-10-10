/*
cleanup.go deletes tar files from the partners' S3 receiving buckets
after those files have been successfully ingested.

If you want to clean up failed bits of multipart S3 uploads in the
preservation bucket, see multiclean.go.
*/
package main

import (
	"flag"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"github.com/bitly/go-nsq"
)


func main() {
	procUtil := createProcUtil()
	consumer, err := createNsqConsumer(&procUtil.Config)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	bagCleanup := workers.NewBagCleanup(procUtil)
	consumer.SetHandler(bagCleanup)
	consumer.ConnectToNSQLookupd(procUtil.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

func createProcUtil() (procUtil *bagman.ProcessUtil) {
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	bagman.LoadCustomEnvOrDie(customEnvFile, nil)
	procUtil = bagman.NewProcessUtil(requestedConfig)
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}
	procUtil.MessageLog.Info("Bag Cleanup started")
	return procUtil
}

func createNsqConsumer(config *bagman.Config) (*nsq.Consumer, error) {
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(config.MaxCleanupAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "30m")
	return nsq.NewConsumer(config.CleanupTopic, config.CleanupChannel, nsqConfig)
}
