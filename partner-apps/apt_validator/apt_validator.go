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
		fmt.Fprintf(os.Stderr, "Please specify a bag to validate. ")
		fmt.Fprintf(os.Stderr, "Or use apt_validator -help for help.\n")
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("apt_validator <path1> <path2> ... <pathN>")
	fmt.Println("Validates bags for APTrust.")
	fmt.Printf("Each path param should be the path to a tar file,")
	fmt.Println("or the path to a directory that ")
	fmt.Println("you want to tar up and send to APTrust.\n")
	fmt.Println("Examples:")
	fmt.Println("    apt_validator /home/josie/university.edu.my_archive.tar")
	fmt.Println("    apt_validator university.edu.archive_one.tar university.edu.archive_two.tar")
	fmt.Println("    apt_validator /home/josie/university.edu.my_archive/\n")
	printSpecUrl()
}

func printSpecUrl() {
	fmt.Println("The full APTrust bagit specification is available at")
	fmt.Println("https://sites.google.com/a/aptrust.org/aptrust-wiki/technical-documentation/processing-ingest/aptrust-bagit-profile")
}
