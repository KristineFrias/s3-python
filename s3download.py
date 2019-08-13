#!/usr/bin/env python
"""
The script will download the file from s3 bucket.

For usage help:
$ s3download.py --help

Pip package dependencies:
  boto3
  pytz
"""

import logging
import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
import argparse
import sys
import os
from os import path
import pytz

# Set these value for the bucket, region and profile name credentials at AWS account
BUCKET_NAME = 'cfe-ops-test'
DEF_REGION = 'us-west-2'
DEF_PROFILE = 'tin'


#Download a file
def download_file(bucket, file, save_as_path, s3_client):
  logging.basicConfig(level=logging.DEBUG,format='%(levelname)s: %(asctime)s: %(message)s')

  try:
    response = s3_client.download_file(bucket, file, save_as_path)


  except ClientError as e:
    logging.error(e)
    abort()

def abort():
  print 'Aborting run.'
  sys.exit(1)

def main():
  parser = argparse.ArgumentParser(description='Perform downloading a file/directory from S3 bucket')
  parser.add_argument('-b', '--bucket', help='AWS Bucket where the file/directory will download')
  parser.add_argument('-R', '--region', help='AWS Region')
  parser.add_argument('-p', '--profile', help='AWS credentials profile to use')
  parser.add_argument('file', help='Name of the file from s3 bucket to be download')
  parser.add_argument('save_as_path',
    help='A current path with filename where to save the file from S3 bucket \n**use .tar.gz extension if needs to be download is a directory \n**use .gz extension if need to be download is a file')

  args = parser.parse_args()

  bucket = args.bucket or BUCKET_NAME
  region = args.region or DEF_REGION
  profile = args.profile or DEF_PROFILE

  session = boto3.Session(profile_name=profile, region_name=region)
  client = session.client('s3')
  s3 = boto3.client('s3')

  download_file(bucket, args.file, args.save_as_path, client)
  logging.info('Done downloaded the file')
  
if __name__ == '__main__':
  main()

