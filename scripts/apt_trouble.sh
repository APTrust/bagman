#!/bin/bash
#
# Run apt_trouble
#
# This script is run by supervisord. See /etc/supervisord/conf.d/aptrust.conf
#
# Load bash_profile first to set correct PATH and environment
# variables that contain Fluctus login and API key.
source /home/ubuntu/.bash_profile
cd /home/ubuntu/go/src/github.com/APTrust/bagman/apps/apt_trouble
go run apt_trouble.go -config=demo
