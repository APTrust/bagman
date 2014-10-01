# bagman

Server side code for managing BagIt bags sent to and managed by APTrust.

## Prerequisites

Install libmagic first. If you're on a Mac, do it this way:

```
	brew install libmagic
```

If you're on Linux, use apt-get or yum to install libmagic-dev:

```
	sudo apt-get install libmagic-dev
```

Install these prerequisites as well:

```
	go get launchpad.net/goamz
	go get github.com/nu7hatch/gouuid
	go get github.com/bitly/go-nsq
	go get github.com/rakyll/magicmime
	go get github.com/mipearson/rfw
	go get github.com/op/go-logging
	go get github.com/APTrust/bagins
```

## Installation

It's as easy as:

```
	go get github.com/APTrust/bagman
```

## Configuration

You must have the environment variables AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY in your environment for bagman to connect
to S3.

You can create a named custom configuration in config.json, and
then run bagman with that named configuration using:

```
	cd cli
	go run cli.go -config=apd4n
```

... or whatever named configuration you want. If you dont specify a
configuration, the cli program will print a list of available
configurations.

You can run bagman locally with the following command. This will pull
down files <= 200k and process them. You will likely have to change
the TarDirectory and LogDirectory in the config.json file to paths
that actually exist on your system.

At the moment, bagman retrieves files from some hard-coded bucket
names that you won't be able to access unless you are using the
APTrust AWS account.

## Testing

```
	go test
```

Bagman will skip the S3 integration tests if it can't find your AWS
keys in your environment, or if it can't get a response from the
Fluctus server at the FluctusURL specified in config.json. The S3
tests should succeed if you have any AWS keys in your environment,
since the integration tests read from a public bucket.

## Running Locally with NSQ

See the run_demo.sh script in the scripts directory. This runs all of
the services required for bag processing, using the "demo"
configuration settings in the config.json file. To run everything
locally, you can set up your own config secion in config.json, then
run one of the scripts in the scripts directory, using your newly
created config.

The config settings you'll need to change to run locally are
TarDirectory, LogDirectory and RestoreDirectory. These should all
point to existing directories in which you have read/write access. You
should also set MaxFileSize to a relatively low value (100000 -
250000), so that you only fetch and process relatively small files
from the S3 receiving buckets. If MaxFileSize is set too high, or if
it's set to zero, which means no limit, you will wind up downloading
and processing an enormous amount of data.

The scripts directory also includes process_items.sh and
restore_items.sh. These are good for running local end-to-end
integration tests. To run these on your own machine, you'll have to
change the config setting in those scripts from apd4n to whatever
config name you've set up. You also *must* have Fluctus running
locally (or at whatever URL the FluctusURL setting points to).

The process_items.sh script runs the bucket reader to look for items
in the receiving buckets, then ingests them. The restore_items.sh
script looks for items marked for delete and restore, and deletes or
restores them.

A good way to run end-to-end tests locally is to run process_items.sh,
then go to http://localhost:3000 and mark a few intellectual objects
for delete and a few for restore. Then kill process_items.sh and run
restore_items.sh. Both scripts print occasional **STATS** lines that
show the number of items that have processed successfully and the
number that have failed.

## NSQ

The nsq folder contains service.go, which is a wrapper around NSQ. It
also contains config files with settings for the NSQ demon. See the
README in the nsq directory for more information.

## The Apps

Bag processing, deletion and restoration requires a number of
apps. Here's a breakdown of the apps and what they do:

### Bucket Reader

*apps/bucket_reader* runs as a cron job on the Go utility server. It
 scans all of the partner intake buckets, then checks the Processed
 Items list in Fluctus to see which of these items have not yet
 started the ingest process. It adds unprocessed tar files to the
 prepare_topic in NSQ.

### Prepare

*apps/apt_prepare* handles the first stages on ingest. It reads from
 NSQ's prepare_channel, which often contains duplicate
 entries. apt_prepare throws out duplicates, then downloads tar files
 from the receiving buckets, unpacks them, and validates them. It
 makes an entry for each valid bag in NSQ's store_topic. Invalid bags
 are recorded in Fluctus' Processed Items list as invalid. apt_prepare
 deletes the original tar file from the local file system when it
 finishes, but it leaves the untarred files for apt_store to work
 with.

### Store

*apps/apt_store* reads from the store_channel and stores the generic
 files unpacked by apt_prepare in the S3 preservation bucket (aka
 long-term storage). If it's not able to store all the files in S3, it
 records an error in Fluctus' Processed Items list. Otherwise, it
 creates an entry in NSQ's metadata_topic so that data about the bag
 can be recorded in Fluctus. apt_store deletes all of the untarred
 contents of the bag from the local file system when it finishes.

### Record

*apps/apt_record* reads from NSQ's metadata_channel, which contains
 information about where all of a bag's files have been stored. It
 records this data in Fluctus (Fedora), creating or updating the
 Intellectual Object, the Generic Files and Premis Events. apt_record
 puts successfully ingested items into NSQ's cleanup_topic.

### Cleanup

*apps/apt_cleanup* reads from NSQ's cleanup_channel to see which
 successfully-ingested tar files should be deleted from the partner's
 receiving buckets. It deletes those files from the S3 buckets.

### Request Reader

*apps/request_reader* runs as a cron job on the Go utility server. It
 checks the Processed Items list in Fluctus for 1) intellectual
 objects that users want to restore, and 2) generic files that users
 want to delete. It puts these items into NSQ's restore_topic and
 delete_topic.

### Restoration

*apps/apt_restore* reads from NSQ's restore_channel and restores
 intellectual objects. It fetches all of the object's generic files
 from the preservation bucket, reassembles them into a bag (a tar
 file), and puts them into the owning institution's restoration
 bucket. (If the CustomRestoreBucket option in config.json is set to
 some other bucket name, the object will be restored to that bucket.)
 Multi-part bags will not be divided into the same units as the
 original upload, but the entire bag, once reassembled from the parts
 will be complete. (E.g. A bag may have been uploaded in 100 parts,
 and it may be restored in 20 parts. None of the restored parts will
 match the original parts, but the restored whole will match the
 original whole.)

### Deletion

*apps/apt_delete* reads from NSQ's delete_channel and deletes generic
 files from the S3 preservation bucket. Copies of the deleted files
 will remain in Glacier storage for a period of time after deletion.
