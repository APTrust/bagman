#!/bin/bash
#
# Run all of the bagman tests

BAGMAN_HOME=~/go/src/github.com/APTrust/bagman

echo "Loading ~/.bash_profile. Assuming AWS environment vars are set there"
. ~/.bash_profile
echo "Testing bagman"
cd $BAGMAN_HOME
go test
echo "Testing fluctus/models"
cd ${BAGMAN_HOME}/fluctus/models
go test
echo "Testing fluctus/client"
cd ${BAGMAN_HOME}/fluctus/client
go test
echo "Testing ingesthelper"
cd ${BAGMAN_HOME}/ingesthelper
go test
echo "Testing process util"
cd ${BAGMAN_HOME}/processutil
go test
