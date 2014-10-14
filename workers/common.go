package workers

import (
	"flag"
	"github.com/APTrust/bagman/bagman"
	"github.com/bitly/go-nsq"
	"github.com/op/go-logging"
	"os"
)

// TODO: Write tests for these two.

// Creates and returns a ProcessUtil object for a worker process.
func CreateProcUtil() (procUtil *bagman.ProcessUtil) {
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	bagman.LoadCustomEnvOrDie(customEnvFile, nil)
	procUtil = bagman.NewProcessUtil(requestedConfig)
	err := procUtil.Config.EnsureFluctusConfig()
	if err != nil {
		procUtil.MessageLog.Fatalf("Required Fluctus config vars are missing: %v", err)
	}
	return procUtil
}

// Creates and returns an NSQ consumer for a worker process.
func CreateNsqConsumer(config *bagman.Config, workerConfig *bagman.WorkerConfig) (*nsq.Consumer, error) {
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", workerConfig.MaxInFlight)
	nsqConfig.Set("heartbeat_interval", workerConfig.HeartbeatInterval)
	nsqConfig.Set("max_attempts", workerConfig.MaxAttempts)
	nsqConfig.Set("read_timeout", workerConfig.ReadTimeout)
	nsqConfig.Set("write_timeout", workerConfig.WriteTimeout)
	nsqConfig.Set("msg_timeout", workerConfig.MessageTimeout)
	return nsq.NewConsumer(workerConfig.NsqTopic, workerConfig.NsqChannel, nsqConfig)
}

// Initializes basic services for a reader fills the queues.
// Readers such as the bucket_reader and request_reader run
// as cron jobs. They read from external sources (Fluctus,
// S3 buckets, etc.) then add messages to the appropriate
// NSQ topic when they find work to be done.
//
// Returns a MessageLog for the reader to log messages and
// a FluctusClient for the reader to read from Fluctus.
//
// Will die if it cannot find the requested config file, or
// if essential config options (such as where to find Fluctus)
// are missing.
func InitializeReader() (*logging.Logger, *bagman.FluctusClient, error) {
	requestedConfig := flag.String("config", "", "Configuration to run. Options are in config.json file. REQUIRED")
	customEnvFile := flag.String("env", "", "Absolute path to file containing custom environment vars. OPTIONAL")
	flag.Parse()
	config := bagman.LoadRequestedConfig(requestedConfig)
	messageLog := bagman.InitLogger(config)
	bagman.LoadCustomEnvOrDie(customEnvFile, messageLog)
	messageLog.Info("Request reader started")
	fluctusClient, err := bagman.NewFluctusClient(
		config.FluctusURL,
		config.FluctusAPIVersion,
		os.Getenv("FLUCTUS_API_USER"),
		os.Getenv("FLUCTUS_API_KEY"),
		messageLog)
	return messageLog, fluctusClient, err
}
