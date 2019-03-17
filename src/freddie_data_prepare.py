# PySpark code to load freddie data correctly

from __future__ import print_function
import boto3
import botocore
import zipfile
import urllib3
import os


def download_zip(filename):
    """
        download zipped files from s3

    """
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

def filename_constructor(year, quater):
    """

        construct file url

    """
    baseurl = "https://freddiemac.embs.com/FLoan/Data/download.php?f=historical_data1_Q"

    return baseurl + str(quater) + str(year)

def download_file(file_url):
    """

        download zipped files from url

    """
    print('Beginning file download with urllib3...')
    urllib3.request.urlopen(file_url)
    download_file(url_1)

def download_all_freddie():
    """

        download all files

    """
    year = range(1999,2018)
    quater = [1,2,3,4]
    for i in year:
        if i!= 2018:
            for j in quater:
            file_url = filename_constructor(i,j)
            download_file(file_url)
        else:
            for j in quater[:3]:
            filr_url = filename_constructor(i,j)
            download_file(file_url)

def zip_unzip(filename):
    """

        unzip the file

    """
    zip = zipfile.ZipFile(filename)
    zip.extractall()
    zip_namelist = zipfile.ZipFile(filename).namelist()
    return zip_namelist

def textfile_upload_remove(filename, zip_namelist):
    """

        upload the unzipped txt files back to s3 and remove the zip file in localhost

    """
    foldername = 'freddie/'
    for txtfile_name in zip_namelist:
        s3.Bucket('onemortgage').upload_file(txtfile_name, foldername + txtfile_name)
        os.remove(txtfile_name)
    os.remove(filename)

def print_ziplist():

    """
        print all the name of zipfiles

    """
    zip_list = []
    for obj in my_bucket.objects.filter(Prefix='freddie/historical'):
        # print('{0}:{1}'.format(my_bucket.name,obj.key))
        zip_list.append(str(obj.key).replace('freddie/', ""))
    return zip_list

def unzip_all_rawzip():

    """

        unzip all zip files in zip list

    """
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket('onemortgage')
    zip_list = print_ziplist()
    for zip_file in zip_list:
        download_zip(zip_file)
        zip_namelist = zip_unzip(zip_file)
        textfile_upload_remove(zip_file, zip_namelist)
    print('upzip job complete!')

if __name__ == '__main__':
    download_all_freddie()
    unzip_all_rawzip()