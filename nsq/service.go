package main

import (
    "os"
    "io"
    "os/exec"
    "fmt"
    "flag"
)

// Start the NSQ services. You can kill then all with Control-C
func main() {
    configFile := flag.String("config", "", "Path to nsqd config file")
    flag.Parse()
    fmt.Println("Config file =", *configFile)
    if configFile == nil {
        fmt.Println("Usage: go run service -config=/path/to/nsq/config")
        fmt.Println("    Starts nsqlookupd, nsqd, and nsqadmin.")
        fmt.Println("    Config files are in dir bagman/nsq")
        fmt.Println("    Ctrl-C stops all of those processes")
        os.Exit(1)
    }
    run(*configFile)
}

// Run each of the services...
func run(configFile string) {
    fmt.Println("Starting NSQ processes. Use Control-C to quit all")
    nsqlookupd := startProcess("nsqlookupd", "")
    nsqd := startProcess("nsqd", fmt.Sprintf("--config=%s",configFile))
    nsqadmin := startProcess("nsqadmin", "--lookupd-http-address=127.0.0.1:4161")

    nsqlookupd.Wait()
    nsqd.Wait()
    nsqadmin.Wait()
}

// Start a process, redirecting it's stderr & stdout so they show up
// in this process's terminal. Returns the command, so we can wait while
// it runs.
func startProcess(command string, arg string) (*exec.Cmd) {
    fmt.Println("Starting", command, arg)
    cmd := exec.Command(command, arg)
    stdout, err := cmd.StdoutPipe()
    if err != nil {
        fmt.Println(err)
    }
    stderr, err := cmd.StderrPipe()
    if err != nil {
        fmt.Println(err)
    }
    go io.Copy(os.Stdout, stdout)
    go io.Copy(os.Stderr, stderr)
    err = cmd.Start()
    if err != nil {
        fmt.Println("Error starting", command, err)
    }
    return cmd
}
