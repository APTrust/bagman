#!/bin/bash
#
# Compress and copy rolled log files to S3.
# First line loads AWS variables out of our bash profile,
# so the AWS CLI tools can get our credentials.
# ----------------------------------------------------------------------
. /home/ubuntu/.bash_profile
DATE=`date +%Y%m%d`
FILE=${1}-${DATE}
gzip $FILE
COMPRESSED_FILE=${FILE}.gz
aws s3 cp --region=us-east-1 $COMPRESSED_FILE s3://aptrust.s3.logs/bag_processing/
