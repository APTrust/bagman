#!/bin/bash

#
# This script provides end-to-end testing for DPN ingest and replication
# functions. To run this test, you'll need to running a local Fluctus
# server and a local DPN cluster. You can accomplish that with these steps:
#
# 1. In your local Fluctus directory, run `rails server`
# 2. In your dpn-server directory, run `script/run_cluster.sh`
# 3. Run this script.
#
# After running this script, you can check the following local apps in your
# browser:
#
# Fluctus               http://localhost:3000
# APTrust DPN Node      http://localhost:3001
# Chronopolis DPN Node  http://localhost:3002
# Hathi Trust DPN Node  http://localhost:3003
# SDR DPN Node          http://localhost:3004
# TDR DPN Node          http://localhost:3005
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
#

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
    exit 1
fi

# Copy bags and transfer requests from other nodes to our local DPN node.
echo "Synching replication requests from remote nodes to local"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_sync
go run dpn_sync.go -config test
if [ $? != 0 ]
then
    echo "DPN sync failed"
    kill -s SIGINT $NSQ_PID
    exit 1
fi

# Start NSQ, because we'll need to put some data into
# the work queues.
echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.dev.config &>/dev/null &
NSQ_PID=$!
sleep 3

# Check for replication requests from other nodes, and
# put them into NSQ for processing. This should find
# the transfer requests created by dpn_test_setup.go
# Let this run to completion. It should take a few seconds.
echo "Checking for replication requests"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_check_requests
go run dpn_check_requests.go -config test
if [ $? != 0 ]
then
    echo "Check requests failed"
    kill_all
    exit 1
fi

# Package processor packages existing APTrust bags for
# ingest to DPN.
echo "Starting the package processor"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_package
go run dpn_package.go -config test &
PACKAGE_PID=$!

# Copy processor copies bags from other nodes via rsync, so
# we can replicate them.
echo "Starting the DPN copy processor"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_copy
go run dpn_copy.go -config test &
COPY_PID=$!

# Start the validation worker, which validates bags we're
# replicating from other nodes.
echo "Starting the DPN validation processor"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_validate
go run dpn_validate.go -config test &
VALIDATION_PID=$!

# Start the record worker, which records the results of bag processing
# in both Fluctus and DPN
echo "Starting the DPN record processor"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_record
go run dpn_record.go -config test &
RECORD_PID=$!

# Start the storage worker, which copies DPN bags to long-term
# storage in AWS S3.
echo "Starting the DPN store worker"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_store
go run dpn_store.go -config test &
STORE_PID=$!

# Start the trouble queue processor. If any bags run into problems,
# our services will dump detailed JSON info into the trouble queue.
echo "Starting the trouble queue processor"
cd ~/go/src/github.com/APTrust/bagman/apps/dpn_trouble
go run dpn_trouble.go -config test &
TROUBLE_PID=$!

kill_all()
{
    echo "Shutting down the package worker"
    kill -s SIGINT $PACKAGE_PID

    echo "Shutting down the copy worker"
    kill -s SIGINT $COPY_PID

    echo "Shutting down the validation worker"
    kill -s SIGINT $VALIDATION_PID

    echo "Shutting down the storage worker"
    kill -s SIGINT $STORE_PID

    echo "Shutting down the record worker"
    kill -s SIGINT $RECORD_PID

    echo "Shutting down the trouble worker"
    kill -s SIGINT $TROUBLE_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGTERM SIGINT SIGHUP

wait $NSQ_PID
