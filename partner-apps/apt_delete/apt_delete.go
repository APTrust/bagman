package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/partner-apps"
	"os"
)

var configFile string
var showHelp bool

func main() {
	parseCommandLine()
	client, err := bagman.NewPartnerS3ClientFromConfigFile(configFile, false)
	if err != nil {
		fmt.Printf("[FATAL] %v\n", err)
		return
	}
	deleteAll(client)
}

func deleteAll(client *bagman.PartnerS3Client) {
	succeeded := 0
	failed := 0
	files := flag.Args()
	bucketName := client.PartnerConfig.RestorationBucket
	for _, file := range files {
		err := client.Delete(bucketName, file)
		if err != nil {
			fmt.Printf("[ERROR] File could not be deleted from S3 " +
				"restoration bucket: %v\n", err)
			failed++
		} else {
			fmt.Printf("[OK]    Deleted %s\n", file)
			succeeded++
		}
	}
	fmt.Printf("Finished. %d succeeded, %d failed\n", succeeded, failed)
}


func parseCommandLine() {
	showVersion := false
	flag.BoolVar(&showVersion, "version", false, "Print version and exit")
	flag.BoolVar(&showHelp, "help", false, "Show help")
	flag.StringVar(&configFile, "config", "", "APTrust config file")
	flag.Parse()
	if showVersion {
		partnerapps.PrintVersion("apt_delete")
		os.Exit(0)
	}
	if showHelp || configFile == "" {
		partnerapps.PrintVersion("apt_delete")
		printUsage()
		os.Exit(0)
	}
	if len(os.Args) < 2 {
		fmt.Printf("Please specify one or more files to delete.\n")
		fmt.Printf("Hint: Use apt_list to see what's in your restoration bucket.\n")
		fmt.Printf("Or use apt_delete -h for help.\n")
		os.Exit(1)
	}
}

func printUsage() {
	message := `
apt_delete --config=pathToConfigFile <file1>...<fileN>

Deletes APTrust bag files from your S3 restoration bucket.
`
	fmt.Println(message)
	fmt.Println(partnerapps.ConfigHelp)
}
