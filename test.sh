#!/bin/bash
#
# Run all of the bagman tests

echo "Loading ~/.bash_profile. Assuming AWS environment vars are set there"
. ~/.bash_profile
echo "Testing bagman"
go test
echo "Testing fluctus/models"
cd fluctus/models
go test
echo "Testing fluctus/client (Is Fluctus running on localhost:3000?)"
cd ../client
go test
echo "Testing ingesthelper"
cd ../../ingesthelper
go test
echo "Testing process util"
cd ../processutil
go test
