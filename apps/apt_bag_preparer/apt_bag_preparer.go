package main

import (
	"flag"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"github.com/bitly/go-nsq"
)

// apt_prepare receives messages from nsqd describing
// items in the S3 receiving buckets. It fetches, untars,
// and validates tar files, then queues them for storage,
// if they untar and validate successfully.
func main() {
	procUtil := createProcUtil()
	consumer, err := createNsqConsumer(&procUtil.Config)
	if err != nil {
		procUtil.MessageLog.Fatalf(err.Error())
	}
	bagPreparer := workers.NewBagPreparer(procUtil)
	consumer.SetHandler(bagPreparer)
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
	procUtil.MessageLog.Info("Bag Preparer started")
	return procUtil
}

func createNsqConsumer(config *bagman.Config) (*nsq.Consumer, error) {
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(config.MaxPrepareAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "180m")
	return nsq.NewConsumer(config.PrepareTopic, config.PrepareChannel, nsqConfig)
}
