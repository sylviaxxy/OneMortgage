# PySpark code to load freddie data correctly
from __future__ import print_function

import boto3
import findspark
import pyspark
from pyspark.sql.functions import col

# sc = SparkContext()
findspark.init()

s3 = boto3.resource('s3')
# BUCKET_NAME = 'onemortgage'
my_bucket = s3.Bucket('onemortgage')


class Worker:

    def __init__(self):
        pass
        self.conf = pyspark.SparkConf()
        self.aws_id = os.environ.get('S3_ACCESS_KEY_ID')
        self.aws_key = os.environ.get('S3_SECRET_ACCESS_KEY')
        self.sc = pyspark.SparkContext()
        self.hadoop_conf = sc._jsc.hadoopConfiguration()
        self.hadoop_conf.set("fs.s3n.awsAccessKeyId", self.aws_id)
        self.hadoop_conf.set("fs.s3n.awsSecretAccessKey", self.aws_key)
        self.s3 = boto3.resource('s3')
        self.sqlContext = pyspark.SQLContext(sc)

    def read_from_s3(self):
        bucket = self.s3.Bucket('gharchive')
        for file in bucket.objects.all():
            file_name, bucket = file.key, file.bucket_name
            datetimes, file_names, string = None, file_name.split('.'), None
            if file_names:
                string = file_names[0]
            if string:
                datetimes = self.parsed(string)

            df = sqlContext.read.json('s3n://' + bucket + '/' + file_name)
            repos = df.filter(
                (col("type") == 'CreateEvent') &
                (col("payload")['object'] == 'repository')) \
                .rdd \
                .map(lambda row: (
                row['id'],
                row['repo']['name'],
                row['created_at'],
                row['public'],
                row['payload']
            )) \
                .collect()
            yield (datetimes, repos)
            break


rom
__future__
import boto3
import botocore
import zipfile
import findspark
import os

# sc = SparkContext()
findspark.init()

s3 = boto3.resource('s3')
# BUCKET_NAME = 'onemortgage'
my_bucket = s3.Bucket('onemortgage')


# print('Loading function')

# use amazon lambda to download data from a list of url
# def lambda_handler(event, context):
# quater = [1,2,3,4]
# year = range(1999,2018)
# url='https://freddiemac.embs.com/FLoan/Data/download.php?f=historical_data1_Q'+str(quater)+str(year) # put your url here
# bucket = 'onemortgage' #your s3 bucket
# key = 'freddie/historical_data1_Q'+str(quater_i)+str(year_i) #your desired s3 path or filename

# s3=boto3.client('s3')
# http=urllib3.PoolManager()
# s3.upload_fileobj(http.request('GET', url,preload_content=False), bucket, key)


# def lambda_handler(event, context):
# inputArt = event["CodePipeline.job"]["data"]["inputArtifacts"][0]
# cred = event["CodePipeline.job"]["data"]["artifactCredentials"]
# sourceBucket = inputArt["location"]["s3Location"]["bucketName"]
# key_name = inputArt["location"]["s3Location"]["objectKey"]
# ACCESS_KEY = cred["secretAccessKey"]
# SECRET_KEY =  cred["accessKeyId"]
# SESSION_TOKEN = cred["sessionToken"]

# s3 = boto3.resource("s3")
# obj = s3.Bucket(sourceBucket).Object(key_name)
# targetBucket = "flightzipper1"

# with io.BytesIO(obj.get()["Body"].read()) as file:
# with zipfile.ZipFile(file, mode='r') as zipf:
# for subfile in zipf.namelist():
# with zipf.open(subfile) as unzipped:
# s3.Bucket(targetBucket).upload_fileobj(unzipped, subfile, ExtraArgs={'ContentType': 'text/html'})

# We are done, lets inform CodePipeline
# codepipeline = boto3.client('codepipeline')
# codepipeline.put_job_success_result(jobId=event["CodePipeline.job"]["id"])

# return "success"

# def lambda_handler(event, context):
# key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
# try:
# obj = s3.get_object(Bucket=bucket, Key=key)
# putObjects = []
# with io.BytesIO(obj["Body"].read()) as tf:
# rewind the file
# tf.seek(0)

# Read the file as a zipfile and process the members
# with zipfile.ZipFile(tf, mode='r') as zipf:
# for file in zipf.infolist():
# fileName = file.filename
# putFile = s3.put_object(Bucket=bucket, Key=fileName, Body=zipf.read(file))
# putObjects.append(putFile)
# print(putFile)


# Delete zip file after unzip
# if len(putObjects) > 0:
# deletedObj = s3.delete_object(Bucket=bucket, Key=key)
# print('deleted file:')
# print(deletedObj)

# except Exception as e:
# print(e)
# print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))

def download_zip(filename):
    # download zipped files from s3, will not used after update
    s3 = boto3.resource('s3')
    BUCKET_NAME = 'onemortgage'
    foldername = 'freddie/'

    KEY = foldername + filename
    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, filename)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise


# def filename_constructor(year, quater):
# baseurl = "https://freddiemac.embs.com/FLoan/Data/download.php?f=historical_data1_Q"
# return baseurl +str(quater)+str(year)

# url_1 = 'https://freddiemac.embs.com/FLoan/Data/download.php?f=historical_data1_Q11999'
# def download_file(file_url):
# download zipped files from url
# import urllib3
# print('Beginning file download with urllib3...')
# urllib3.request.urlopen(file_url)
# download_file(url_1)

# def download_all_freddie():
# year = range(1999,2018)
# quater = [1,2,3,4]
# for i in year:
# if i!= 2018:
# for j in quater:
# file_url = filename_constructor(i,j)
# download_file(file_url)
# else:
# for j in quater[:3]:
# filr_url = filename_constructor(i,j)
# download_file(file_url)

def zip_unzip(filename):
    # unzip the file
    zip = zipfile.ZipFile(filename)
    zip.extractall()
    zip_namelist = zipfile.ZipFile(filename).namelist()
    return zip_namelist


def textfile_upload_remove(filename, zip_namelist):
    # upload the unzipped txt files back to s3 and remove the zip file in localhost
    foldername = 'freddie/'
    for txtfile_name in zip_namelist:
        s3.Bucket('onemortgage').upload_file(txtfile_name, foldername + txtfile_name)
        os.remove(txtfile_name)
    os.remove(filename)


def print_ziplist():
    zip_list = []
    for obj in my_bucket.objects.filter(Prefix='freddie/historical'):
        # print('{0}:{1}'.format(my_bucket.name,obj.key))
        zip_list.append(str(obj.key).replace('freddie/', ""))
    return zip_list


def unzip_all_rawzip():
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('onemortgage')
    zip_list = print_ziplist()
    for zip_file in zip_list:
        download_zip(zip_file)
        zip_namelist = zip_unzip(zip_file)
        textfile_upload_remove(zip_file, zip_namelist)
    print('upzip job complete!')


unzip_all_rawzip()
