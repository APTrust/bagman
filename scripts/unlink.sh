#!/bin/bash

# Get rid of links in node outbound directories that point
# to files that don't exist in /mnt/dpn/staging.

dirs=("/home/dpn.chron/outbound" "/home/dpn.hathi/outbound" "/home/dpn.sdr/outbound" "/home/dpn.tdr/outbound")
for dir in ${dirs[@]}
do
    echo $dir
    for f in `ls $dir`
    do
        tarfile=$(basename "$f")
        if ! [ -e /mnt/dpn/staging/$tarfile ]; then
            echo "DELETING $dir/$f"
            rm "$dir/$f"
        fi
    done
done
