#!/bin/bash

#
# This script provides end-to-end testing for DPN ingest and replication
# functions. To run this test, you'll need to running a local Fluctus
# server and a local DPN cluster. You can accomplish that with these steps:
#
# 1. In your local Fluctus directory, run `rails server`
# 2. In your DPN-REST/dpnode directory, run `run_cluster.sh`
# 3. Run this script.
#
# After running this script, you can check the following local apps in your
# browser:
#
# Fluctus               http://localhost:3000
# APTrust DPN Node      http://localhost:8000
# Chronopolis DPN Node  http://localhost:8001
# Hathi Trust DPN Node  http://localhost:8002
# SDR DPN Node          http://localhost:8003
# TDR DPN Node          http://localhost:8004
#

# TODO
#
# - Make sure bags exist in a staging area we can reach. Since we
#   already have test bags in bagman/dpn/testdata, create symlinks
#   to those under ~/tmp/dpn_home/integration_test/outgoing.
# - For rsync copy, overwrite link username with a name that
#   is valid on this system.
# - Overwrite node URLs and API Tokens to work for the local
#   DPN cluster.
#
# 1. Mark a few APTrust bags for replication to DPN.
# 2.

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.dev.config &>/dev/null &
NSQ_PID=$!
sleep 3



kill_all()
{
    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
