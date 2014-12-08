package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/partner-apps"
	"os"
)

var configFile string
var checksum string
var showHelp bool
var deleteFiles bool

func main() {
	parseCommandLine()
 	client, err := bagman.NewPartnerS3ClientFromConfigFile(configFile, false)
	if err != nil {
		fmt.Printf("[FATAL] %v\n", err)
		return
	}
	fetchAll(client)
}

func fetchAll(client *bagman.PartnerS3Client) {
	succeeded := 0
	failed := 0
	files := flag.Args()
	bucketName := client.PartnerConfig.RestorationBucket
	fmt.Printf("Downloading %d files to %s\n", len(files), client.PartnerConfig.DownloadDir)
	for _, file := range files {
		digest, err := client.DownloadFile(bucketName, file, checksum)
		if err != nil {
			fmt.Printf("[ERROR] Failed to download %s: %v\n", file, err)
			failed++
			continue
		}
		deleteMessage := ""
		if deleteFiles {
			err = client.Delete(bucketName, file)
			if err != nil {
				deleteMessage = fmt.Sprintf("File could not be deleted from S3 " +
					"restoration bucket after download: %v", err)
			} else {
				deleteMessage = "File was deleted from S3 restoration bucket."
			}
		}
		if checksum == "none" {
			fmt.Printf("[OK]    Downloaded %s. %s\n", file, deleteMessage)
		} else {
			fmt.Printf("[OK]    Downloaded %s with %s: %s %s\n", file, checksum, digest, deleteMessage)
		}
		succeeded++
	}
	fmt.Printf("Finished. %d succeeded, %d failed\n", succeeded, failed)
}


func parseCommandLine() {
	showVersion := false
	flag.BoolVar(&showVersion, "version", false, "Print version and exit")
	flag.BoolVar(&showHelp, "h", false, "Show help")
	flag.BoolVar(&deleteFiles, "delete", false, "Delete files from restoration bucket after download")
	flag.StringVar(&configFile, "config", "", "APTrust config file")
	flag.StringVar(&checksum, "checksum", "", "Checksum to calculate on download (md5 or sha256). Default is none.")
	flag.Parse()
	if showVersion {
		partnerapps.PrintVersion("apt_download")
		os.Exit(0)
	}
	if showHelp || configFile == "" {
		partnerapps.PrintVersion("apt_download")
		printUsage()
		os.Exit(0)
	}
	if len(os.Args) < 2 {
		fmt.Printf("Please specify one or more files to download. ")
		fmt.Printf("Or use apt_upload -h for help.\n")
		os.Exit(1)
	}
	if checksum != "" && checksum != "md5" && checksum != "sha256" && checksum != "none" {
		fmt.Printf("checksum must be md5, sha256 or none. Default is none.")
		os.Exit(1)
	}
	if checksum == "" {
		checksum = "none"
	}
}

func printUsage() {
	message := `
apt_download [--checksum=<md5|sha256>] [--delete] --config=pathToConfigFile <file1>...<fileN>

Downloads APTrust bag files from the S3 restoration bucket.
You must first request bag restoration through the APTrust Web UI.
Once you are notified that the bag has been restored, you can
download it with apt_download.

The checksum param is optional. If omitted, no checksum digest will
be calculated on the downloaded file. Valid checksum options are:

md5       Calculates the md5 digest
sha256    Calculated the sha256 digest
none      Does not calculate any digest. This is the default, and
          this will be applied if you omit the -checksum flag.

If you supply the --delete flag, files will be deleted from the S3
restoration bucket after download.

apt_download prints all output to stdout. Typical output includes the
result of the file download (OK or ERROR) and the md5 or sha256 checksum,
if you requested that on the command line.

Output looks like this:

Downloading 2 files to /home/josie/downloads
[OK]    Downloaded archive1.tar with md5: 845f9be9c825b668f3bae1d23ceb01de
[OK]    Downloaded archive2.tar with md5: 3d4fede4b748a5d8acbdefd13f39d0cd
Finished. 2 succeeded, 0 failed
`
	fmt.Println(message)
	fmt.Println(partnerapps.ConfigHelp)
}
