#!/usr/bin/python
import os
import math
import json
import re
import subprocess
import boto3
import pyspark
import psycopg2
from pyspark.sql.functions import col

from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
sc = pyspark.SparkContext()
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
s3 = boto3.resource('s3')
sqlContext = pyspark.SQLContext(sc)

def read_text_file(url_pattern,real_colnames):
    data = sc.textFile(url_pattern)
    data_1 = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
    df = sqlContext.createDataFrame(data_1)
    for c , n in zip(df.columns, real_colnames):
        df = df.withColumnRenamed(c , n)
    return df

#def parse_config(configfile):
    #"""
    #reads configs saved as json record in configuration file and returns them
    #:type configfile: str       path to config file
    #:rtype          : dict      configs
    #"""
    #conf = json.load(open(configfile, "r"))
    #return replace_envvars_with_vals(conf)


#def replace_envvars_with_vals(dic):
    #"""
    #for a dictionary dic which may contain values of the form "$varname",
    #replaces such values with the values of corresponding environmental variables
    #:type dic: dict     dictionary where to parse environmental variables
    #:rtype   : dict     dictionary with parsed environmental variables
    #"""
    #for el in dic.keys():
        #val = dic[el]
        #if type(val) is dict:
            #val = replace_envvars_with_vals(val)
        #else:
            #if type(val) in [unicode, str] and len(val) > 0 and '$' in val:
                #command = "echo {}".format(val)
                #dic[el] = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read().strip()
    #return dic
