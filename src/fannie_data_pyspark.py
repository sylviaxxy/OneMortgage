# PySpark code to process fannie mae data
pyspark --packages org.postgresql:postgresql:9.4.1212 --master spark://ip-10-0-0-4:7077

import os
import re
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

data_fannie = sc.textFile('s3a://onemortgage/fannie/Acquisition_*.txt')
data_1_f = data_fannie.map(lambda x: x.split('|'))
data_2_f = data_1_f.map(lambda x: [i.encode('utf-8') for i in x])

df2 = sqlContext.createDataFrame(data_2_f)


real_colnames_fannie = [
  "loan_seq_no",
  "channel",
  "seller_name",
  "original_interest_rate",
  "original_upb",
  "original_loan_term",
  "origination_date",
  "first_payment_dat",
  "original_ltv",
  "original_cltv",
  "number_of_borrowers",
  "dti numeric",
  "credit_score",
  "first_time_homebuyer_indicator",
  "loan_purpose",
  "property_type",
  "number_of_units",
  "occupancy_status",
  "property_state",
  "zip_code",
  "mip",
  "product_type",
  "co_borrower_credit_score",
  "mortgage_insurance_type",
  "relocation_mortgage_indicator"]

for c,n in zip(df2.columns,real_colnames_fannie):
    df2 = df2.withColumnRenamed(c,n)

# save_to_postgresql on AWS RDS
url = "jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql"
df2.write.format("jdbc").option("url", url).option("driver","org.postgresql.Driver").option("dbtable", "fannie_origination").option("dbname", "pgsql").option("user", "sylviaxuinsight").option("password", "568284947Aa").option("numPartitions", "10000").mode("overwrite").save()

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
