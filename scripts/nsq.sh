#!/bin/bash
#
# Run NSQ services
# Load bash_profile first to set correct PATH
#
source /home/ubuntu/.bash_profile
cd /home/ubuntu/go/src/github.com/APTrust/bagman/nsq/
go run service.go -config /home/ubuntu/go/src/github.com/APTrust/bagman/nsq/nsqd.demo.config
