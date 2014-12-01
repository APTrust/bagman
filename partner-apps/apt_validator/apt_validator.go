package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"os"
)

var showHelp bool

func main() {
	validateCommandLine()
	anyBagFailed := false
	for i := 1; i < len(os.Args); i++ {
		filePath := os.Args[i]
		validator, err := bagman.NewValidator(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating validator for %s: %s\n", filePath, err)
			os.Exit(1)
		}
		if validator.IsValid() {
			fmt.Printf("[PASS] %s is a valid APTrust bag\n", filePath)
		} else {
			fmt.Printf("[FAIL] %s is not valid for the following reasons:\n", filePath)
			fmt.Println(" ", validator.ErrorMessage)
			anyBagFailed = true
		}
	}
	if anyBagFailed {
		fmt.Println("")
		printSpecUrl()
		os.Exit(1)
	}
}

func validateCommandLine() {
	flag.BoolVar(&showHelp, "h", false, "Show help")
	flag.Parse()
	if showHelp {
		printUsage()
		os.Exit(0)
	}
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Please specify one or more bags to validate. ")
		fmt.Fprintf(os.Stderr, "Or use apt_validator -h for help.\n")
		os.Exit(1)
	}
}

func printUsage() {
	usage := `
apt_validator <path1> <path2> ... <pathN>
Validates bags for APTrust.
Each path param should be the path to a tar file, or the path to a directory
that you want to tar up and send to APTrust.

  Examples:
    apt_validator /home/josie/university.edu.my_archive.tar
    apt_validator university.edu.archive_one.tar university.edu.archive_two.tar
    apt_validator /home/josie/university.edu.my_archive/
`
	fmt.Println(usage)
	printSpecUrl()
}

func printSpecUrl() {
	fmt.Println("The full APTrust bagit specification is available at")
	fmt.Println("https://sites.google.com/a/aptrust.org/aptrust-wiki/technical-documentation/processing-ingest/aptrust-bagit-profile")
}
