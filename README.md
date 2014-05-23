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
keys in your environment. The S3 tests should succeed if you have any
AWS keys in your environment, since the integration tests read from a
public bucket.

## Running with NSQ

This is currently in its earliest stages. You'll need few terminal
sessions for this. In one terminal, run this with the correct config
path:

```
cd nsq
go run service.go -config=/path/to/bagman/nsq/nsqd.apd4n.config
```

In another terminal, run this, to put a few items in the queue:

```
cd bucket_reader
go run bucket_reader.go -config=apd4n
```

In a third terminal, run this:

```
cd processor
go run bag_processor.go -config=apd4n
```

Results will be in the log files.