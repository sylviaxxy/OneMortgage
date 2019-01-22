# PySpark code to load, merge, clean freddie data
import zipfile
import io
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf

def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return dict(zip(files, [file_obj.open(file).read() for file in files]))


zips = sc.binaryFiles("s3:/Testing/*.zip") #arn:aws:s3:::onemortgage
files_data = zips.map(zip_extract).collect()

spark = SparkSession.builder
                        .master("local")
                        .appName("app name")
                        .config("spark.some.config.option", true).getOrCreate()

df = spark.read.parquet("s3://path/to/parquet/file.parquet")
