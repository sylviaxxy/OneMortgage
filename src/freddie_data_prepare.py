# PySpark code to load, merge, clean freddie data

# unzip and upload all text files back to s3
import findspark
import os
import io
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkFiles
#sc = SparkContext()
findspark.init()

import boto3
import botocore
import zipfile

s3 = boto3.resource('s3')

#BUCKET_NAME = 'onemortgage'
my_bucket = s3.Bucket('onemortgage')

def download_zip(filename):

    # download zipped files

    s3 = boto3.resource('s3')
    BUCKET_NAME = 'onemortgage'
    foldername = 'freddie/'

    KEY = foldername+filename

    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, filename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise

def zip_unzip(filename):
    # unzip the file
    zip = zipfile.ZipFile(filename)
    zip.extractall()
    zip_namelist = zipfile.ZipFile(filename).namelist()
    return zip_namelist

def textfile_upload_remove(filename,zip_namelist):
    # upload the unzipped txt files back to s3 and remove the zip file in localhost
    foldername = 'freddie/'
    for txtfile_name in zip_namelist:
        s3.Bucket('onemortgage').upload_file(txtfile_name, foldername+txtfile_name)
        os.remove(txtfile_name)
    os.remove(filename)

def print_ziplist():
    zip_list = []
    for obj in my_bucket.objects.filter(Prefix='freddie/historical'):
        #print('{0}:{1}'.format(my_bucket.name,obj.key))
        zip_list.append(str(obj.key).replace('freddie/', ""))
    return zip_list

def unzip_all_rawzip():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('onemortgage')
    zip_list = print_ziplist()
    for zip_file in zip_list:
        download_zip(zip_file)
        zip_namelist = zip_unzip(zip_file)
        textfile_upload_remove(zip_file,zip_namelist)
    print('upzip job complete!')

unzip_all_rawzip()
