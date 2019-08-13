#!/usr/bin/env python
#
#The script will get all the files from s3 bucket.
#
#For usage help:
#$ s3list.py --help
 #
 #Pip package dependencies:
 #  boto3
 # pytz


import logging
import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
import argparse
import sys
import os
from os import path

# Set these value for the bucket, region and profile name credentials at AWS account

#DEF_REGION = 'us-west-2'
#DEF_PROFILE = 'tin'


#Download a file
def download_file(bucket_name, s3_resource):

  try:
    for object in bucket_name.objects.all():
      print(object)

  except ClientError as e:
    abort(e)

def abort():
  print 'Aborting run.'
  sys.exit(1)

def main():
  parser = argparse.ArgumentParser(description='Get the list of files from S3 bucket')
  parser.add_argument('-R', '--region', help='AWS Region')
  parser.add_argument('-p', '--profile', help='AWS credentials profile to use')
  parser.add_argument('-b', '--bucket', help='AWS Bucket where the file/directory will get')
    
  args = parser.parse_args()
 
  region = args.region 
  profile = args.profile 
  bucket = args.bucket

  session = boto3.Session(profile_name=profile, region_name=region)
  client = session.client('s3')
  s3_resource = boto3.resource('s3')
  bucket_name = s3_resource.Bucket(bucket)

  download_file(bucket_name, s3_resource)
    
if __name__ == '__main__':
  main()

