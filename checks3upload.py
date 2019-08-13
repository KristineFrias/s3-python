#!/usr/bin/env python
#
#The script will check if the file was uploaded successfully 
#by returning the last modified file in s3 bucket
#
#For usage help:
#$ checkuploadfile.py --help
#
#Pip package dependencies:
#  boto3
#  pytz


import boto3
from boto3.session import Session
from botocore.exceptions import ClientError
import argparse
from datetime import datetime

# Set these value for the bucket, region and profile name credentials at AWS account
#BUCKET_NAME = '<bucket_name>'
#DEF_REGION = '<region>'
#DEF_PROFILE = '<profile_name>'

#Check the the last modified file in S3 bucket
def check_uploadfile(bucket_name, s3_resource):
  last_modified_date = datetime(1939, 9, 1).replace(tzinfo=None)
  
  try:
    for file in bucket_name.objects.all():
      file_date = file.last_modified.replace(tzinfo=None)    
      if last_modified_date < file_date:
        last_modified_date = file_date

    for file in bucket_name.objects.all():
      if file.last_modified.replace(tzinfo=None) == last_modified_date:
        print(file.key)

  except ClientError as e:
    abort(e)

def abort():
  print 'Aborting run.'
  sys.exit(1)

def main():
  parser = argparse.ArgumentParser(description='Check the file or directory if successfully uploaded to s3 bucket')
  parser.add_argument('-b', '--bucket', help='AWS Bucket where to check the uploaded file/directory')
  parser.add_argument('-R', '--region', help='AWS Region')
  parser.add_argument('-p', '--profile', help='AWS credentials profile to use')

  args = parser.parse_args()

  bucket = args.bucket
  region = args.region
  profile = args.profile

  session = boto3.Session(profile_name=profile, region_name=region)
  s3_resource = boto3.resource('s3')
  bucket_name = s3_resource.Bucket(bucket)

  check_uploadfile(bucket_name, s3_resource)
  
if __name__ == '__main__':
  main()
