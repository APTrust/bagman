#!/bin/bash

# Create symlinks in the outbound directories of replicating nodes.
# These all point to items in the staging directory.

for f in `ls /mnt/dpn/staging/*.tar`
do
    tarfile=$(basename "$1")
    ln -s $f /home/dpn.chron/outbound/$tarfile
    ln -s $f /home/dpn.hathi/outbound/$tarfile
    ln -s $f /home/dpn.sdr/outbound/$tarfile
    ln -s $f /home/dpn.tdr/outbound/$tarfile
done
