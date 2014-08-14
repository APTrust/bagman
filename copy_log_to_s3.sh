#!/bin/bash
#
# Copy rolled log files to S3.
# ----------------------------------------------------------------------
aws s3 cp --region=us-east-1 $1 s3://aptrust.s3.logs/bag_processing/
