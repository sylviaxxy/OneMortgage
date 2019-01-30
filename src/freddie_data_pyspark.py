# Use pyspark to process the freddie mac data
import os
import re
import boto3
import pyspark
import psycopg2
from pyspark.sql.functions import col

conf = pyspark.SparkConf()
aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
sc = pyspark.SparkContext()
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
s3 = boto3.resource('s3')
sqlContext = pyspark.SQLContext(sc)

# monthly performance files looks like 'historical_data1_time_Q11999.txt'
# data = sc.textFile('s3a://onemortgage/freddie/historical_data1_time_Q[1-4][0-9]{4}.txt')
data = sc.textFile('s3a://onemortgage/freddie/historical_data1_time_Q*.txt')
data_1 = data.map(lambda x: x.split('|')).map(lambda x: x[0])
data_1.take(10)

real_colnames_monthly = ["loan_seq_no", "mon_rep_period", "cur_actual_upb",
                         "cur_delinquency", "loan_age", "mon_to_maturity",
                         "repurchase", "modification", "zero_balance_code",
                         "zero_balance_date", "cur_interest_rate", "cur_deferred_upb",
                        "ddlpi", "mi_recoveries", "net_sale_proceeds", "non_mi_recoveries",
                        "expenses", "legal_costs", "maintain_costs", "tax_insurance",
                        "miscellaneous_expenses","actual_loss", "modification_cost",
                        "step_modification", "deferred_payment", "estimaed_ltv"]

for c,n in zip(df.columns,real_colnames_monthly):
    df = df.withColumnRenamed(c,n)

def define_submit_args():
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--driver-class-path /home/ubuntu/postgresql-42.2.5.jar \
    --jars /home/ubuntu/postgresql-42.2.5.jar pyspark-shell'

data = sc.textFile('s3a://onemortgage/freddie/historical_data1_Q*.txt')
data_1 = data.map(lambda x: x.split('|'))
data_2 = data_1.map(lambda x: [str(i) for i in x])

def col_names():
    real_colnames = ['credit_score','first_payment_date',
        'first_time_homebuyer_flag','maturity_date','msa',
        'mip', 'num_units', 'occupancy_status' , 'ori_cltv',
        'ori_dti', 'ori_upb', 'ori_ltv', 'ori_interest_rate',
        'channel', 'prepayment_penalty_flag', 'product_type', 'property_state', 'property_type',
        'postal_code', 'loan_seq_no', 'loan_purpose', 'ori_term',
        'num_borrower','seller_name', 'servicer_name', 'super_conforming_flag', 'pre_harp_loan_seq_no'] #only harp data have the last col
    return real_colnames

def sql_context_read():

    df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter","|"). \
                load("s3n://onemortgage/freddie/historical_data1_Q11999.txt")

def formalize_colnames(real_colnames):
    for c,n in zip(df.columns,real_colnames):
        df = df.withColumnRenamed(c,n)
    return df

df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter","|"). \
                load("s3a://onemortgage/freddie/historical_data1_Q11999.txt")

# save_to_postgresql on AWS RDS
url = "jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/mortgagepgsql"
df.write \
    .format("jdbc") \
    .option("url", url) \
    .option("driver","org.postgresql.Driver") \
    .option("dbtable", "freddie_origin") \
    .option("dbname", "mortgagepgsql") \s
    .option("user", "sylviaxuinsight") \
    .option("password", "564210126Aa") \
    .mode("append")



df22 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter","|"). \
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
