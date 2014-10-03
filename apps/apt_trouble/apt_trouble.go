package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/bagman"
	"github.com/bitly/go-nsq"
	"github.com/op/go-logging"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// Global vars.
var config bagman.Config
var messageLog *logging.Logger

func main() {

	loadConfig()
	messageLog.Info("Trouble started")
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("heartbeat_interval", "10s")
	nsqConfig.Set("max_attempts", uint16(config.MaxMetadataAttempts))
	nsqConfig.Set("read_timeout", "60s")
	nsqConfig.Set("write_timeout", "10s")
	nsqConfig.Set("msg_timeout", "10m")
	consumer, err := nsq.NewConsumer(config.TroubleTopic,
		config.TroubleChannel, nsqConfig)
	if err != nil {
		messageLog.Fatalf(err.Error())
	}

	handler := &RecordProcessor{}
	consumer.SetHandler(handler)
	consumer.ConnectToNSQLookupd(config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
}

func loadConfig() {
	// Load the config or die.
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	config = bagman.LoadRequestedConfig(requestedConfig)
	messageLog = bagman.InitLogger(config)
	bagman.LoadCustomEnvOrDie(customEnvFile, messageLog)
}

type RecordProcessor struct {
}

func (*RecordProcessor) HandleMessage(message *nsq.Message) error {
	var result bagman.ProcessResult
	err := json.Unmarshal(message.Body, &result)
	if err != nil {
		detailedError := fmt.Errorf(
			"Could not unmarshal JSON data from nsq: %v. JSON: %s",
			err, string(message.Body))
		messageLog.Error(detailedError.Error())
		message.Finish()
		return detailedError
	}
	dumpToFile(&result)
	messageLog.Info("Processed %s", result.S3File.Key.Key)
	return nil
}

func dumpToFile(result *bagman.ProcessResult) error {
	outdir := path.Join(config.LogDirectory, "trouble")
	if _, err := os.Stat(outdir); os.IsNotExist(err) {
		err := os.Mkdir(outdir, 0766)
		if err != nil {
			panic(err)
		}
	}
	filename := fmt.Sprintf("%s_%s",
		bagman.OwnerOf(result.S3File.BucketName),
		strings.Replace(result.S3File.Key.Key, ".tar", ".json", -1))
	json, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(path.Join(outdir, filename), json, 0644)
	if err != nil {
		panic(err)
	}
	return nil
}
