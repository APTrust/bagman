package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/APTrust/bagman/partner-apps"
	"github.com/crowdmob/goamz/s3"
	"os"
	"strings"
)

var configFile string
var bucket string
var limit int
var showHelp bool

func main() {
	parseCommandLine()
	client, err := bagman.NewPartnerS3ClientFromConfigFile(configFile, false)
	if err != nil {
		fmt.Printf("[FATAL] %v\n", err)
		return
	}
	bucketName := client.PartnerConfig.RestorationBucket
	if bucket == "receiving" {
		bucketName = client.PartnerConfig.ReceivingBucket
	}
	fmt.Printf("Listing up to %d items from bucket %s\n", limit, bucketName)
	keys, err := client.List(bucketName, limit)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if len(keys) == 0 {
		fmt.Printf("Bucket %s is empty\n", bucket)
		return
	}
	fmt.Printf("%-24s  %-32s  %-16s  %s\n", "LastModified", "ETag", "Size", "File")
	printItems(keys)
}

func printItems(keys []s3.Key) {
	for i := range keys {
		key := keys[i]
		md5 := strings.Replace(key.ETag, "\"", "", 2)
		fmt.Printf("%-24s  %-32s  %-16d  %s\n", key.LastModified, md5, key.Size, key.Key)
	}
}

func parseCommandLine() {
	showVersion := false
	flag.BoolVar(&showVersion, "version", false, "Print version and exit")
	flag.BoolVar(&showHelp, "help", false, "Show help")
	flag.StringVar(&configFile, "config", "", "APTrust config file")
	flag.StringVar(&bucket, "bucket", "restoration", "The bucket to list: receiving or restoration")
	flag.IntVar(&limit, "limit", 100, "Max number of items to list")
	flag.Parse()
	if showVersion {
		partnerapps.PrintVersion("apt_list")
		os.Exit(0)
	}
	if showHelp {
		partnerapps.PrintVersion("apt_list")
		printUsage()
		os.Exit(0)
	}
	if configFile == "" {
		if partnerapps.DefaultConfigFileExists() {
			configFile, _ = partnerapps.DefaultConfigFile()
			fmt.Printf("Using default config file %s\n", configFile)
		} else {
			partnerapps.PrintVersion("apt_list")
			printUsage()
			os.Exit(0)
		}
	}
	if bucket != "restoration" && bucket != "receiving" {
		fmt.Printf("bucket must be either receiving or restoration\n")
		os.Exit(1)
	}
	if limit <= 0 {
		fmt.Printf("No point in listing %d items. I quit!\n", limit)
		os.Exit(1)
	}
}

func printUsage() {
	message := `
apt_list [--config=pathToConfigFile] --bucket=<restoration|receiving> [--limit=100]

Lists the contents of your APTrust receiving bucket or restoration
bucket.

You may omit the --config option if you want to use the default
config file in your home directory (~/.aptrust_partner.conf).

The bucket argument is required, and must be either restoration or
receiving.

The limit option describes the maximum number of items to list.

apt_list prints all output to stdout. Output includes the name,
size, etag and last modified date of each file.

For files under 5GB, the etag is the file's md5 checksum. For files
larger than 5GB, the etag represents a digest of the md5 sums from
each part of a multipart upload, and will not match your original
file's md5 checksum. More information on how etags are calculated
for large files is available at http://bit.ly/12BH7ti

`
	fmt.Println(message)
	fmt.Println(partnerapps.ConfigHelp)
}
