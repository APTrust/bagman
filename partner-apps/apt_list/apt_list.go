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
		fmt.Printf("Bucket %s is empty", bucket)
		return
	}
	fmt.Printf("%-24s  %-32s  %-16s  %s\n", "LastModified", "Md5", "Size", "File")
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
	flag.BoolVar(&showHelp, "h", false, "Show help")
	flag.StringVar(&configFile, "config", "", "APTrust config file")
	flag.StringVar(&bucket, "bucket", "restoration", "The bucket to list: receiving or restoration")
	flag.IntVar(&limit, "limit", 100, "Max number of items to list")
	flag.Parse()
	if showVersion {
		partnerapps.PrintVersion("apt_list")
		os.Exit(0)
	}
	if showHelp || configFile == "" {
		partnerapps.PrintVersion("apt_list")
		printUsage()
		os.Exit(0)
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
apt_list -config=pathToConfigFile -bucket=<restoration|receiving> [-limit=100]

Lists the contents of your APTrust receiving bucket or restoration
bucket.

The bucket argument is required, and must be either restoration or
receiving.

The limit option describes the maximum number of items to list.

apt_list prints all output to stdout. Output includes the name,
size, md5 checksum and last modified date of each file.

`
	fmt.Println(message)
	fmt.Println(partnerapps.ConfigHelp)
}
