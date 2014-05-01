package bagman

import (
	"fmt"
	"io/ioutil"
	"os"
	"encoding/json"
)

type Config struct {
	TarDirectory   string
	LogDirectory   string
	MaxFileSize    int64
	LogLevel       LogLevel
	Workers        int
	FluctusURL     string
}

// This returns the configuration that the user requested.
// If the user did not specify any configuration (using the
// -config flag), or if the specified configuration cannot
// be found, this prints a help message and terminates the
// program.
func LoadRequestedConfig(requestedConfig *string) (config Config) {
	configurations := loadConfigFile()
	config, configExists := configurations[*requestedConfig]
	if requestedConfig == nil || !configExists  {
		printConfigHelp(*requestedConfig, configurations)
		os.Exit(1)
	}
	return config
}

// This prints the current configuration to stdout.
func PrintConfig(config Config) {
	fmt.Println("Running with the following configuration:")
	fmt.Printf("    Tar Directory: %s\n", config.TarDirectory)
	fmt.Printf("    Log Directory: %s\n", config.LogDirectory)
	fmt.Printf("    Log Level:     %d\n", config.LogLevel)
	fmt.Printf("    Max File Size: %d\n", config.MaxFileSize)
	fmt.Printf("    Workers:       %d\n", config.Workers)
	fmt.Printf("Output will be logged to bagman_json and bagman_messages in %s\n", config.LogDirectory)
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
	file, err := ioutil.ReadFile("../config.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		os.Exit(1)
	}
	err = json.Unmarshal(file, &configurations)
	if err != nil{
		fmt.Fprint(os.Stderr, "Error parsing JSON from config file:", err)
		os.Exit(1)
	}
	return configurations
}
