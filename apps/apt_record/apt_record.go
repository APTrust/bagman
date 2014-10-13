package main

import (
	"flag"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/workers"
	"github.com/bitly/go-nsq"
)

/*
apt_record records bag metadata in Fluctus, including
info about Intellectual Objects, Generic Files and Premis Events.
*/
func main() {
	procUtil := createProcUtil()
	consumer, err := createNsqConsumer(&procUtil.Config)
	if err != nil {
		procUtil.MessageLog.Fatal(err.Error())
	}
	bagRecorder := workers.NewBagRecorder(procUtil)
	consumer.SetHandler(bagRecorder)
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
	procUtil.MessageLog.Info("Bag Recorder started")
	return procUtil
}

func createNsqConsumer(config *bagman.Config) (*nsq.Consumer, error) {
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", config.RecordWorkers * 10)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(config.MaxMetadataAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "60m")
	return nsq.NewConsumer(config.MetadataTopic, config.MetadataChannel, nsqConfig)
}
