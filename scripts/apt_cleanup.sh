#!/bin/bash
#
# Run apt_cleanup
#
# This script is run by supervisord. See /etc/supervisord/conf.d/aptrust.conf
#
# Load bash_profile first to set correct PATH and environment
# variables that contain Fluctus login and API key.
source /home/ubuntu/.bash_profile
cd /home/ubuntu/go/src/github.com/APTrust/bagman/apps/apt_cleanup
go run apt_cleanup.go -config=demo
