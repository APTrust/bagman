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
    go run cli.go -config=dev
```

... or whatever named configuration you want.

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
