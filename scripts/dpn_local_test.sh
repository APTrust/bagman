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

# Load some test data into the local DPN cluster.
# Let this one run to completion, which should take
# just a few seconds.
echo "Setting up test bags for transfer"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_test_setup
go run dpn_test_setup.go -config test
if [ $? != 0 ]
then
    echo "Test data setup failed"
    exit
fi

# Start NSQ, because we'll need to put some data into
# the work queues.
echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.dev.config &>/dev/null &
NSQ_PID=$!
sleep 3


# Copy bags and transfer requests from other nodes to our local DPN node.
echo "Synching replication requests from remote nodes to local"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_sync
go run dpn_sync.go -config dpn/dpn_config.json
if [ $? != 0 ]
then
    echo "DPN sync failed"
    exit
fi


# Check for replication requests from other nodes, and
# put them into NSQ for processing. This should find
# the transfer requests created by dpn_test_setup.go
# Let this run to completion. It should take a few seconds.
echo "Checking for replication requests"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_check_requests
go run dpn_check_requests.go -config dpn/dpn_config.json
if [ $? != 0 ]
then
    echo "Check requests failed"
    exit
fi


kill_all()
{
    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
