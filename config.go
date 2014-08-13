package bagman

import (
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	"os"
	"path/filepath"
)

type Config struct {
	// ActiveConfig is the configuration currently
	// in use.
	ActiveConfig string

	// TarDirectory is the directory in which we will
	// untar files from S3. This should be on a volume
	// with lots of free disk space.
	TarDirectory string

	// LogDirectory is where we'll write our log files.
	LogDirectory string

	// If true, processes will log to STDERR in addition
	// to their standard log files. You really only want
	// to do this in development.
	LogToStderr bool

	// LogLevel is defined in github.com/op/go-logging
	// and should be one of the following:
	// 1 - CRITICAL
	// 2 - ERROR
	// 3 - WARNING
	// 4 - NOTICE
	// 5 - INFO
	// 6 - DEBUG
	LogLevel logging.Level

	// MaxFileSize is the size in bytes of the largest
	// tar file we're willing to process. Set to zero
	// to process all files, regardless of size.
	// Set to some reasonably small size (100000 - 500000)
	// when you're running locally, or else you'll wind
	// up pulling down a huge amount of data from the
	// receiving buckets.
	MaxFileSize int64

	// Fetchers is the number of goroutines to use when
	// fetching files from the receiving buckets.
	Fetchers int

	// Workers is the number of goroutines to run for
	// untarring and processing bags. This should be
	// set to something close to the number of CPU cores.
	Workers int

	// FluctusURL is the URL of the Fluctus server where
	// we will be recording results and metadata. This should
	// start with http:// or https://
	FluctusURL string

	// The version of the Fluctus API we're using. This should
	// start with a v, like v1, v2.2, etc.
	FluctusAPIVersion string

	// Buckets is a list of S3 receiving buckets to check
	// for incoming tar files.
	Buckets []string

	// NsqdHttpAddress is the address of the NSQ server.
	// We can put items into queues by issuing PUT requests
	// to this URL. This should start with http:// or https://
	NsqdHttpAddress string

	// NsqLookupd is the hostname and port number of the NSQ
	// lookup deamon. It should not include a protocol.
	// E.g. localhost:4161. Queue consumers use this to
	// discover available queues.
	NsqLookupd string

	// BagProcessorTopic is the name of the NSQ topic the bag
	// processor will read from. The bucket_reader pushes
	// messages into this topic.
	BagProcessorTopic string

	// BagProcessorChannel is the name of the NSQ channel the
	// bag processor will read.
	BagProcessorChannel string

	// Maximum number of times we'll try to process a bag.
	MaxBagAttempts int

	// MetaDataTopic is the name of the NSQ topic the bag
	// processor sends results to. The metadata_processor worker
	// will read from this topic. (Not yet implemented)
	MetadataTopic string

	// MetaDataChannel is the name of the NSQ channel from which
	// the metadata_processor reads. (Not yet implemented)
	MetadataChannel string

	// Maximum number of times we'll try to send metadata to Fluctus.
	MaxMetadataAttempts int

	// CleanupTopic contains messages about files that need to
	// be deleted from S3.
	CleanupTopic string

	// CleanupChannel is what cleanup.go listens to to find out
	// which files it should delete from S3.
	CleanupChannel string

	// Maximum number of times we'll try to clean up files in S3.
	MaxCleanupAttempts int

	// TroubleTopic is the name of the NSQ topic to
	// which the bag_processor and metarecord processes send
	// info about items that were partially processed and
	// then left in an inconsistent state.
	TroubleTopic string

	// TroubleChannel is the name of the NSQ channel that
	// holds information about the state of partially-processed
	// bags that need attention from an administrator.
	TroubleChannel string

	// Maximum number of times we'll try to process a trouble item.
	MaxTroubleAttempts int

	// SkipAlreadyProcessed indicates whether or not the
	// bucket_reader should  put successfully-processed items into
	// NSQ for re-processing. This is amost always set to false.
	// The exception is when we deliberately want to reprocess
	// items to test code changes.
	SkipAlreadyProcessed bool

	// The name of the preservation bucket to which we should
	// copy files for long-term storage.
	PreservationBucket string

	// Should we delete the uploaded tar file from the receiving
	// bucket after successfully processing this bag?
	DeleteOnSuccess bool
}

func (config *Config) AbsLogDirectory() string {
	absPath, err := filepath.Abs(config.LogDirectory)
	if err != nil {
		msg := fmt.Sprintf("Cannot get absolute path to log directory. "+
			"config.LogDirectory is set to '%s'", config.LogDirectory)
		panic(msg)
	}
	return absPath
}

// This returns the configuration that the user requested.
// If the user did not specify any configuration (using the
// -config flag), or if the specified configuration cannot
// be found, this prints a help message and terminates the
// program.
func LoadRequestedConfig(requestedConfig *string) (config Config) {
	configurations := loadConfigFile()
	config, configExists := configurations[*requestedConfig]
	if requestedConfig == nil || !configExists {
		printConfigHelp(*requestedConfig, configurations)
		os.Exit(1)
	}
	config.ActiveConfig = *requestedConfig
	return config
}

// This prints a message to stdout describing how to specify
// a valid configuration.
func printConfigHelp(requestedConfig string, configurations map[string]Config) {
	fmt.Fprintf(os.Stderr, "Unrecognized config '%s'\n", requestedConfig)
	fmt.Fprintln(os.Stderr, "Please specify one of the following configurations:")
	for name, _ := range configurations {
		fmt.Println(name)
	}
	os.Exit(1)
}

// This function reads the config.json file and returns a list of
// available configurations.
func loadConfigFile() (configurations map[string]Config) {
	file, err := LoadRelativeFile("config.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		os.Exit(1)
	}
	err = json.Unmarshal(file, &configurations)
	if err != nil {
		fmt.Fprint(os.Stderr, "Error parsing JSON from config file:", err)
		os.Exit(1)
	}
	return configurations
}

func (config *Config) EnsureFluctusConfig() error {
	if config.FluctusURL == "" {
		return fmt.Errorf("FluctusUrl is not set in config file")
	}
	if os.Getenv("FLUCTUS_API_USER") == "" {
		return fmt.Errorf("Environment variable FLUCTUS_API_USER is not set")
	}
	if os.Getenv("FLUCTUS_API_KEY") == "" {
		return fmt.Errorf("Environment variable FLUCTUS_API_KEY is not set")
	}
	return nil
}
