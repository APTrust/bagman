# Utility Server

Here's how to set up a utility server to test out bagman. These
instructions descibe the setup process for a small server, which is
good for test runs because 1) it's cheap, and 2) it comes with 160GB
of attached storage, which is handy because bagman will be working
with some fairly large files.

Assuming you've already spun up an EC2 small running a 32-bit Ubuntu
12.04 or 14.04 AMI, follow the instructions below.

Install an editor if you don't like working with vim. If you like vim,
just kick back and relax.

```
    sudo apt-get install emacs23-nox golang-mode
```

The "go get" command uses git, bazaar, and mercurial to download
packages.  Install those, along with libmagic-dev, which we need
for determining file mime types. We'll need gcc to build the
magicmime go package later.

We also need mailutils so that cron jobs can send email, and
alpine is handy for reading email:

```
    sudo apt-get install git bzr mercurial
    sudo apt-get install gcc libmagic-dev
    sudo apt-get install mailutils
    sudo apt-get install alpine
```

Install go, but don't use apt-get.

```
    curl "https://storage.googleapis.com/golang/go1.2.2.linux-amd64.tar.gz" > go1.2.2.linux-amd64.tar.gz
    sudo tar -C /usr/local -xzf go1.2.2.linux-amd64.tar.gz
```

Create a GOHOME directory:

```
    mkdir ~/go
```

Create ~/.bash_profile with the following:

```
    export EDITOR=emacs
    export AWS_ACCESS_KEY_ID="Our access key id"
    export AWS_SECRET_ACCESS_KEY="Our secret key"
    export GOPATH=$HOME/go
    export PATH="$GOPATH/bin:$PATH:/usr/local/go/bin"
```

Run this to load the bash profile:

```
    source ~/.bash_profile
```

Now if you run this:

```
    go version
```

You should see this output:

```
    go version go1.2.1 linux/386
```

Mount the EBS volume and then make directories to hold the tar/bag
files we're going to download.

```
    sudo mkdir /mnt/apt
    sudo mount /dev/xvdc /mnt/apt
    sudo mkdir /mnt/apt/data
    sudo mkdir /mnt/apt/logs
    sudo chown ubuntu.ubuntu /mnt/apt/data/
    sudo chown ubuntu.ubuntu /mnt/apt/logs/
```

Install the go packages we'll need:

```
	go get github.com/diamondap/goamz/aws
	go get github.com/diamondap/goamz/s3
    go get github.com/nu7hatch/gouuid
    go get github.com/rakyll/magicmime
    go get github.com/APTrust/bagins
    go get github.com/APTrust/bagman
    go get github.com/op/go-logging
    go get github.com/mipearson/rfw
```

You can also copy a version of bagman from your local machine, like
so:

```
    scp -r ~/go/src/github.com/APTrust/bagman/ ubuntu@apt-util:go/src/github.com/APTrust/
```

Note that this assumes you have an entry in your local ~/.ssh/config
like this:

```
    Host apt-util
        User ubuntu
        Port 22
        IdentityFile ~/.ssh/MyPrivateKey.pem
        TCPKeepAlive yes
        IdentitiesOnly yes
        HostName ec2-54-85-73-179.compute-1.amazonaws.com
```

And remember to check the AWS console for the *actual* public IP
address of your EC2 instance.

Bagman has a ‘dev’ configuration and a ‘test’ configuration. Use the
dev configuration when running on your local machine. This will
retrieve only smaller files (<200k) from S3. Use the ‘test’
configuration when running on an EC2 instance. That will retrieve all
files in all the S3 buckets.

You can now run the test code like this:

```
    cd ~/go/src/github.com/APTrust/apmanager/test
    go run test.go -config=test
```

Bagman will print out some information about its configuration and it
will tell you where it’s writing its log files. You can tail the logs
if you want to watch its progress.

You'll see a json log, which is thorough but not very readable, and
a messages log, which prints simple info and error messages. That
one will give you an overview of what the program is actually
doing.

The JSON log, by the way, records the actual results of every
significant operation the program performed during its run. Someday
soon, there will be code that uses the JSON log to report on results,
and possibly to reconstruct any data that did not make it into
fluctus.

After running the test program, make sure it cleaned up after itself:

```
    ls /mnt/apt/data/
```

That listing should show an empty directory.

The log should be in /mnt/apt_logs. You will have to delete that
manually.

## Setting up NSQ

If you're running NSQ on a 64-bit machine, you can get pre-built
binaries at http://nsq.io/deployment/installing.html. Just untar the
latest package and copy the executables onto your path.

If you're running on a 32-bit machine, you'll need to follow these steps
to build the nsq files.

```
go get github.com/tools/godep
go get github.com/bmizerany/assert
godep get github.com/bitly/nsq/...
cd /home/ubuntu/go/src/github.com/bitly/nsq
./test.sh
```

The last step builds all of the necessary nsq components before
it tests them.

## Avoiding S3 Connection Reset Errors

Large S3 file downloads fail often with the message "Connection reset
by peer." A desciption of the problem, along with a solution appears
here:

http://scie.nti.st/2008/3/14/amazon-s3-and-connection-reset-by-peer/

__Note that as of May 22, 2014, these fixes have not yet been applied
to the server, since they slow throughput. We will wait and see if
they are needed.__

Here's a quick summary of the fix, in case the link dies. Make sure
the following lines are in /etc/sysctl.conf:

```
# Workaround for TCP Window Scaling bugs in other ppl's equipment:
net.ipv4.tcp_wmem = 4096 16384 512000
net.ipv4.tcp_rmem = 4096 87380 512000
```

Then run this:

```
sudo sysctl -p
```

You may also have to run this:

```
sudo service procps start
```

## Optional Steps for Running Fluctus

To run Fluctus on the utility server, you'll need to install Ruby
2.1.1, a Postgres client, and a few other items. This is optional, and
is not something you would do in production. But for testing purposes,
it allows you to run the full ingest stack on a single server.

Install the Postgres client and the dev libraries. Rails will and
bundler will need these. Rails also needs nodejs for the JavaScript
runtime.

```
sudo apt-get install postgresql-client libpq-dev nodejs unzip
```

Install rbenv and ruby-build. You'll need a recent ruby-build; the apt
package for Ubuntu 14.04 does not have a build definition for Ruby 2.1.1.

```
sudo apt-get install rbenv
git clone https://github.com/sstephenson/ruby-build.git
~/.rbenv/plugins/ruby-build
```

Now add /home/ubuntu/.rbenv/plugins/ruby-build/bin to your PATH in
.bash_profile. Your.bash_profile should include the lines below. Cut
and paste if you have to.

<pre>
export EDITOR=emacs
export AWS_ACCESS_KEY_ID="XXX"
export
AWS_SECRET_ACCESS_KEY="XXX"
export GOPATH=$HOME/go
export
PATH="$GOPATH/bin:$PATH:/usr/local/go/bin:/home/ubuntu/.rbenv/plugins/ruby-build/bin"
export RAILS_ENV=demo
export FLUCTUS_DB_USER="XXX"
export
FLUCTUS_DB_HOST="XXX"
export REDIS_HOST=localhost
export REDIS_PORT=6379
export FEDORA_URL="http://localhost:8983/fedora"
export FEDORA_USER="XXX"
export FEDORA_PASS="XXX"
export FEDORA_NAMESPACE="aptrust-demo"
export FLUCTUS_API_USER="XXX"
export FLUCTUS_API_KEY="XXX"
eval "$(rbenv init -)"
</pre>

Reload your .bash_profile:

```
source ~/.bash_profile
```

Install Ruby 2.1.2 (2.1.1 has a build problem. See
https://bugs.ruby-lang.org/issues/9578):

```
rbenv install 2.1.2
```

Check out fluctus from GitHub, switch to the right branch and set the
rbenv version:

```
git clone -b feature/processingQueue git@github.com:APTrust/fluctus.git ~/fluctus
cd ~/fluctus
rbenv local 2.1.2
rbenv rehash
sudo apt-get install bundler
```

Notice that bundle is a system install. Running "gem install bundler"
to install bundler into the local 2.1.2 Ruby installation gives you a
bundler that does not seem to work.

Install Oracle Java 7 from webupd8.org:

```
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java7-installer
java -version
```

The last command should show Java 1.7 as the version.

In the ~/fluctus directory, run the following:

```
bundle install
rake secret
```

Run the rake secret twice, copying the secrets into the
RAILS_SECRET_KEY and DEVISE_SECRET_KEY in
~/fluctus/config/application.yml.

Your application.yml file should look something like this:

<pre>
demo: &demo
  GOOGLE_KEY:
  GOOGLE_SECRET:
  SOLR_URL: http://localhost:8983/solr/development
  FEDORA_URL: http://localhost:8983/fedora
  FEDORA_USER: XXX
  FEDORA_PASS: XXX
  FEDORA_NAMESPACE: aptrust-demo
  REDIS_HOST: localhost
  REDIS_PORT: 6379
  RAILS_SECRET_KEY: XXXXXXXX
  DEVISE_SECRET_KEY: XXXXXXXXX

test:
  &lt;&lt;: *demo
</pre>

Install jetty/hydra by running this command in ~/fluctus:

```
rails g hydra:jetty
rake jetty:start
rake db:migrate
rake fluctus:populate_db
```

Once all this is done (phew!), run the following in separate screen
sessions. Session rails:

```
cd ~/fluctus
rake jetty:start
rails server
```

Session nsq:

```
cd ~/go/src/github.com/APTrust/bagman/nsq
go run service.go
-config=/home/ubuntu/go/src/github.com/APTrust/bagman/nsq/nsqd.demo.config
```

Session bucket_reader:

```
cd ~/go/src/github.com/APTrust/bagman/bucket_reader
go run bucket_reader.go -config=demo
```

Session bag_processor:

```
cd ~/go/src/github.com/APTrust/bagman/processor
go run bag_processor.go -config=demo
```
