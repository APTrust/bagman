#!/bin/bash

if [ "$HOSTNAME" = "dpn-demo.aptrust.org" ]; then
    START_DIR=$PWD
    cd /var/www/dpn-server
    bundle exec rake integration:reset
    /home/ubuntu/go/src/github.com/APTrust/bagman/bin/dpn_sync -config=demo
    /home/ubuntu/go/src/github.com/APTrust/bagman/bin/dpn_check_requests -config=demo
    cd $START_DIR
    echo 'Test is complete. Run one of these commands:'
    echo 'tail -f /mnt/apt/logs/dpn_copy.log'
    echo 'tail -50 /mnt/apt/logs/dpn_copy.log'
else
    echo 'This script is only for dpn-demo.aptrust.org'
fi
