#!/bin/bash

#
# This script provides end-to-end testing for APTrust's restore
# and delete features. To run this, run process_items.sh first.
# See the comment at the top of the process_items.sh script.
# Running process_items.sh, followed by this script, exercises
# ALL of APTrust's major features.
#

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go -config ~/go/src/github.com/APTrust/bagman/nsq/nsqd.dev.config &>/dev/null &
NSQ_PID=$!
sleep 3

# Wait for this one to finish
echo "Starting request reader"
cd ~/go/src/github.com/APTrust/bagman/apps/request_reader
go run request_reader.go -config dev

echo "Starting bag restorer"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_restore
go run apt_restore.go -config dev &
RESTORE_PID=$!

echo "Starting generic file deleter"
cd ~/go/src/github.com/APTrust/bagman/apps/apt_file_delete
go run apt_file_delete.go -config dev &
DELETE_PID=$!

kill_all()
{
    echo "Shutting down deleter"
    kill -s SIGINT $DELETE_PID

    echo "Shutting down bag restorer"
    kill -s SIGINT $RESTORE_PID

    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID
}

trap kill_all SIGINT

wait $NSQ_PID
