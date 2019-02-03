# PySpark code to process raw data
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


# Process Freddie Mac data
data_new = sc.textFile('s3a://onemortgage/freddie/historical_data1_time_Q*.txt')
data_1_new = data.map(lambda x: x.split('|')).map(lambda x: x[0])
data_1_new.take(10)

real_colnames_monthly = ["loan_seq_no", "report_period", "cur_actual_upb",
                         "cur_delinquency", "loan_age", "mon_to_maturity",
                         "repurchase", "modification", "zero_balance_code",
                         "zero_balance_date", "cur_interest_rate", "cur_deferred_upb",
                        "ddlpi", "mi_recoveries", "net_sale_proceeds", "non_mi_recoveries",
                        "expenses", "legal_costs", "maintain_costs", "tax_insurance",
                        "miscellaneous_expenses","actual_loss", "modification_cost",
                        "step_modification", "deferred_payment_modification", "estimated_ltv"]



for c,n in zip(df.columns,real_colnames_monthly):
    df = df.withColumnRenamed(c,n)

def define_submit_args():
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--driver-class-path /home/ubuntu/postgresql-42.2.5.jar \
    --jars /home/ubuntu/postgresql-42.2.5.jar pyspark-shell'



data = sc.textFile('s3a://onemortgage/freddie/historical_data1_Q*.txt')
data_1 = data.map(lambda x: x.split('|'))
data_2 = data_1.map(lambda x: [i.encode('utf-8') for i in x])

df = sqlContext.createDataFrame(data_2)

df.printSchema()
df.describe('credit_score').show()
df.describe('property_state').show()
df.describe('first_payment_date').show()


df.drop_duplicates()
df.unionAll(df2)
df.distinct()
df.drop('id')
df.dropna(subset=['age', 'name'])  # 传入一个list，删除指定字段中存在缺失的记录
df.fillna({'age':10,'name':'abc'})  # 传一个dict进去，对指定的字段填充
df.groupby('name').agg(F.max(df['age']))

df.select(F.max(df.age))
df.select(F.min(df.age))
df.select(F.avg(df.age)) # 也可以用mean，一样的效果
df.select(F.countDistinct(df.age)) # 去重后统计
df.select(F.count(df.age)) # 直接统计，经试验，这个函数会去掉缺失值会再统计

from pyspark.sql import Window
df.withColumn("row_number", F.row_number().over(Window.partitionBy("a","b","c","d").orderBy("time"))).show() # row_number()函数
bike_change_2days.registerTempTable('bike_change_2days')


def col_names():
    real_colnames = ['credit_score','first_payment_date',
        'first_time_homebuyer_flag','maturity_date','msa',
        'mip', 'num_units', 'occupancy_status' , 'ori_cltv',
        'ori_dti', 'ori_upb', 'ori_ltv', 'ori_interest_rate',
        'channel', 'prepayment_penalty_flag', 'product_type', 'property_state', 'property_type',
        'postal_code', 'loan_seq_no', 'loan_purpose', 'ori_term',
        'num_borrower','seller_name', 'servicer_name', 'super_conforming_flag'] # ,'pre_harp_loan_seq_no' only harp data have the last col
    return real_colnames

def sql_context_read():

    df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter","|"). \
                load("s3n://onemortgage/freddie/historical_data1_Q11999.txt")

for c,n in zip(df.columns,real_colnames):
    df = df.withColumnRenamed(c,n)

def formalize_colnames(real_colnames):
    for c,n in zip(df.columns,real_colnames):
        df = df.withColumnRenamed(c,n)
    return df

df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter","|"). \
                load("s3a://onemortgage/freddie/historical_data1_Q11999.txt")

# save_to_postgresql on AWS RDS
url = "jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql"
df.write.format("jdbc").option("url", url).option("driver","org.postgresql.Driver").option("dbtable", "freddie_origination").option("dbname", "pgsql").option("user", "sylviaxuinsight").option("password", "568284947Aa").option("numPartitions", "10000").mode("overwrite").save()


# Another way to read file using csv packages
df2 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter","|"). \
                load("s3a://onemortgage/freddie/historical_data1_time_Q11999.txt")


PG_HOST = 'mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com'
PG_USER = 'sylviaxuinsight'
PG_PSWD = '568284947Aa'
dbname = 'pgsql'

sql = """CREATE TABLE freddietest2(
        credit_score INT,
        first_payment_date DATE,
        loan_seq_no VARCHAR(50)
        );"""

print('Connecting to DB...')
conn = psycopg2.connect(
        host=PG_HOST,
        database=dbname,
        user=PG_USER,
        password=PG_PSWD
    )
print('Connected.')
cur = conn.cursor()
cur.execute(sql)
#for record in cur.fetchall():
    #print(record)
cur.close()
conn.commit()
print('Table created!')

df2 = df.select("credit_score","first_payment_date","loan_seq_no")

df.freddie_performance = df.select("loan_seq_no","mon_report",
                                    "current_deq","loan_age", "repurchase")

#df2.write \
    #.format("jdbc") \
    #.option("url", "jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/mortgagepgsql") \
    #.option("driver","org.postgresql.Driver") \
    #.option("dbtable", "freddietest") \
    #.option("dbname", "pgsql") \
    #.option("user", "sylviaxuinsight") \
    #.option("password", "568284947Aa") \
    #.mode("overwrite").save()

sql = """INSERT INTO freddietest2(credit_score, first_payment_date, loan_seq_no)
VALUES ('751', '19911003', 'hdahhklsh')"""

print('Connecting to DB...')
conn = psycopg2.connect(
        host=PG_HOST,
        database=dbname,
        user=PG_USER,
        password=PG_PSWD
    )
print('Connected.')
cur = conn.cursor()
cur.execute(sql)
#for record in cur.fetchall():
    #print(record)
cur.close()
conn.commit()
conn.close()

sql = """SELECT * FROM freddietest2"""
print('Connecting to DB...')
conn = psycopg2.connect(
        host=PG_HOST,
        database=dbname,
        user=PG_USER,
        password=PG_PSWD
    )
print('Connected.')
cur = conn.cursor()
cur.execute(sql)
for record in cur.fetchall():
    print(record)
cur.close()
conn.commit()
conn.close()

sc.stop()

# Useful snippets
df_3 = df_1.join(df_2, on = "id", how = 'outer').select("id", u_get_uniform(df_1["uniform"], \
                 df_2["uniform"]).alias("uniform"), "normal", "normal_2").orderBy(F.col("id")

PG_HOST = 'mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com'
PG_USER = 'sylviaxuinsight'
PG_PSWD = '568284947Aa'
dbname = 'pgsql'

print('Connecting to DB...')
conn = psycopg2.connect(
        host=PG_HOST,
        database=dbname,
        user=PG_USER,
        password=PG_PSWD
    )
print('Connected.')
cursor = conn.cursor()

for line in data_sample:
    query = 'INSERT INTO freddie_origination VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    data =[]
    for i in range(0,26):
        data.append(line[i])
    data = tuple(data)
    cursor.execute(query, data)

for record in cursor.fetchall():
    print(record)
conn.commit()
conn.close()


real_colnames = ['credit_score','first_payment_date',
'first_time_homebuyer_flag','maturity_date','msa',
'mip', 'num_units', 'occupancy_status' , 'ori_cltv',
'ori_dti', 'ori_upb', 'ori_ltv', 'ori_interest_rate',
'channel', 'prepayment_penalty_flag', 'product_type', 'property_state', 'property_type',
'postal_code', 'loan_seq_no', 'loan_purpose', 'ori_term',
'num_borrower','seller_name', 'servicer_name', 'super_conforming_flag']

# Process Fannia Mae
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
