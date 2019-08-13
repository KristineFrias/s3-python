#!/usr/bin/env python
"""
If file_path is a file, gzip will compress the file, then upload to S3.
If file_path is a dir, tarfile will compress the directory, then upload to S3.
Use the gzip.open to create a zip archive and compress the file
Use the tarfile module to create a zip archive of a directory.
For tarfile it will compress the contents of directory in a file and store it in the current directory.

The script will create an archieve gzip file or tarball of a directory 
if the needed arguments will pass by providing AWS credentials and a bucket
then will delete that archieve file or directory after uploading to s3 bucket is fully completed.

For usage help:
$ s3upload.py --help

Pip package dependencies:
  boto3
  pytz
"""

import logging
import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
from datetime import datetime
import argparse
import sys
import os
from os import path
import pytz
import gzip
import tarfile
import shutil

# Set these value for the bucket, region and profile name credential at AWS account
BUCKET_NAME = '<bucket_name>'
DEF_REGION = '<region>'
DEF_PROFILE = '<profile_name>'

#Gzip the file path
def gz_file(path):
  with open(path, 'rb') as file_input:
    with gzip.open(path + '.gz', 'wb') as archieve:
      shutil.copyfileobj(file_input, archieve)

#Tarball the directory path
def tar_dir(path):
  with tarfile.open(os.path.basename(os.path.dirname(path)) + '.tar.gz', 'w:gz') as archieve:
      archieve.add(path) 

#Upload a file or directory to an S3 bucket
def upload_path(path, bucket, s3_client):
  timestamp = datetime.now(pytz.utc).strftime("%Y%m%d_%H%M%S")
  logging.basicConfig(level=logging.DEBUG,format='%(levelname)s: %(asctime)s: %(message)s')

  try:
    if os.path.isfile(path):
      basename = os.path.basename(path + '.gz')
      gz_file(path)
      response = s3_client.upload_file(path + '.gz', bucket, timestamp + '_' + basename)
      os.remove(path + '.gz')
    else:
      basename = os.path.basename(os.path.dirname(path)) + '.tar.gz'
      tar_dir(path)
      with open(basename, 'rb') as dir_path:
        respone = s3_client.upload_fileobj(dir_path, bucket, timestamp + '_' + basename)
      os.remove(basename)

  except ClientError as e:
    logging.error(e)
    abort()

def abort():
  print 'Aborting run.'
  sys.exit(1)

def main():
  parser = argparse.ArgumentParser(description='Perform uploading a file/directory to S3 bucket')
  parser.add_argument('-b', '--bucket', help='AWS Bucket where the file/directory will upload')
  parser.add_argument('-R', '--region', help='AWS Region')
  parser.add_argument('-p', '--profile', help='AWS credential profile to use')
  parser.add_argument('file_path', help='File/Directory path that will upload to S3 bucket')

  args = parser.parse_args()

  bucket = args.bucket or BUCKET_NAME
  region = args.region or DEF_REGION
  profile = args.profile or DEF_PROFILE

  session = boto3.Session(profile_name=profile, region_name=region)
  client = session.client('s3')
  s3 = boto3.client('s3')

  upload_path(args.file_path, bucket, client)
  logging.info('File/Directory was uploaded to S3 bucket')

if __name__ == '__main__':
  main()

