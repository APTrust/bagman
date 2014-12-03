package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/partner-apps"
	"os"
)

var configFile string
var verbose bool
var showHelp bool

func main() {
	parseCommandLine()
 	client, err := bagman.NewPartnerS3ClientFromConfigFile(configFile, verbose)
	if err != nil {
		fmt.Printf("[FATAL] %v\n", err)
		return
	}
	fmt.Printf("Uploading %d files to s3 bucket %s\n", len(flag.Args()), client.PartnerConfig.ReceivingBucket)
	client.UploadFiles(flag.Args())
}


func parseCommandLine() {
	showVersion := false
	flag.BoolVar(&showVersion, "version", false, "Print version and exit")
	flag.BoolVar(&showHelp, "h", false, "Show help")
	flag.BoolVar(&verbose, "v", false, "Verbose - print verbose messages")
	flag.StringVar(&configFile, "config", "", "APTrust config file")
	flag.Parse()
	if showVersion {
		partnerapps.PrintVersion("apt_upload")
		os.Exit(0)
	}
	if showHelp || configFile == "" {
		partnerapps.PrintVersion("apt_upload")
		printUsage()
		os.Exit(0)
	}
	if len(os.Args) < 2 {
		fmt.Printf("Please specify one or more files to upload. ")
		fmt.Printf("Or use apt_upload -h for help.\n")
		os.Exit(1)
	}
}

func printUsage() {
	message := `
apt_upload -config=pathToConfigFile [-v] <file1> <file2> ... <fileN>

Uploads APTrust bag files to S3 so they can be archived in APTrust.
The files you upload should be tar files that conform to the APTrust
bagit specification. You may use apt_validate to make sure your bags
are valid before uploading. The bags you upload will go into the
receiving bucket specified in your config file.

Examples:
    apt_upload -config=aptrust.conf archive1.tar archive2.tar
    apt_upload -config=aptrust.conf ~/my_data/*.tar
    apt_upload -config=aptrust.conf -v ~/my_data/*

When using the * pattern, as in the second and third examples above,
apt_upload will not recurse into sub directories. It will upload
files only, and will skip directories.

apt_upload prints all output to stdout. Typical output includes the
result of the file upload (OK or ERROR). Failed uploads should show
a description of the error. Successful uploads show the md5 checksum
that S3 calculated on receiving the file. Check this against your
local md5 checksum if you want to ensure the file was received
successfully.

Non-verbose output looks like this:

[OK]    S3 returned md5 checksum adae53cf8373b2c6b20a99f8db518e56 for file1.tar
[OK]    S3 returned md5 checksum 4d66f1ec9491addded54d17b96df8c96 for file2.tar
Finished uploading. 2 succeeded, 0 failed.

The -v option will give verbose output, providing additional information
about what's happening.
`
	fmt.Println(message)
	fmt.Println(partnerapps.ConfigHelp)
	fmt.Println(partnerapps.BagSpecMessage)
}
