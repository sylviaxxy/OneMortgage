from __future__ import print_function
import os
import boto3
import botocore
import zipfile
import urllib3
import requests

def download_all_fannie():
    """
    Download all fannie mae files
    :return:
    """
    username = "your_user_name" # change into your fannie mae username
    password = "your_password" # change into your fannie mae password
    beg_data = "2001Q1"
    end_data = "2018Q4"
    begin_year = int(beg_data[0:4])
    beg_quarter = int(beg_data[-1])
    end_year = int(end_data[0:4])
    end_quarter = int(end_data[-1])

    if beg_year < end_year or (beg_year == end_year and beg_quarter <= end_quarter):

        payload = {
            'username': username,
            'password': password
        }

        s = requests.Session()

        s.post('https://loanperformancedata.fanniemae.com/lppub/loginForm.html', data=payload)

        for year in range(beg_year, end_year + 1):
            if year == beg_year:
                start = beg_quarter
                if year == end_year:
                    end = end_quarter
                else:
                    end = 4
            elif year == end_year:
                start = 1
                end = end_quarter
            else:
                start = 1
                end = 4

            for quarter in range(start, end + 1):
                file_name = str(year) + 'Q' + str(quarter) + '.zip'
                file_url = 'https://loanperformancedata.fanniemae.com/lppub/publish_aws?file=' + file_name

                # Make the request and read the response
                print "Downloading", file_name
                with open(file_name, 'wb') as f:
                    f.write(s.get(file_url).content)
                    f.close()
    else:
        print "Beginning quarter given comes after ending quarter given."


def zip_unzip(filename):
    """
    Unzip the file

    """
    zip = zipfile.ZipFile(filename)
    zip.extractall()
    zip_namelist = zipfile.ZipFile(filename).namelist()
    return zip_namelist

def textfile_upload_remove(filename, zip_namelist):
    """
    Upload the unzipped txt files back to s3 and remove the zip file in localhost

    """
    foldername = 'fannie/'
    for txtfile_name in zip_namelist:
        s3.Bucket('onemortgage').upload_file(txtfile_name, foldername + txtfile_name)
        os.remove(txtfile_name)
    os.remove(filename)

def print_ziplist():

    """
    Print all the name of zipfiles

    """
    zip_list = []
    for obj in my_bucket.objects.filter(Prefix='fannie/historical'):
        # print('{0}:{1}'.format(my_bucket.name,obj.key))
        zip_list.append(str(obj.key).replace('fannie/', ""))
    return zip_list

def unzip_all_rawzip():

    """
    Unzip all zip files in zip list

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
    download_all_fannie()
    unzip_all_rawzip()