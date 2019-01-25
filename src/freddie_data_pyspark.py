#
import os
import boto3
import pyspark
import psycopg2
from pyspark.sql.functions import col

os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-class-path /home/ubuntu/postgresql-42.2.5.jar --jars /home/ubuntu/postgresql-42.2.5.jar pyspark-shell'



conf = pyspark.SparkConf()
aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
sc = pyspark.SparkContext()
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
s3 = boto3.resource('s3')
sqlContext = pyspark.SQLContext(sc)


# datafile = sc.textFile("s3n://onemortgage/freddie/historical_data1_Q11999.txt")
real_colnames = ['credit_score','first_payment_date',
        'first_time_buyer','maturity_date','msa',
        'mi_percent', 'num_units', 'occu_status' , 'ori_cltv',
        'ori_dti', 'ori_upa', 'ori_ltv', 'ori_interest',
        'channel', 'ppm', 'prod_type', 'prop_state', 'prop_type',
        'post_code', 'loan_seq_no', 'loan_purpose', 'ori_term',
        'no_borrower','seller', 'service', 'super_conform', 'pre_harp_loan_seq_no']


df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("delimiter","|"). \
                load("s3n://onemortgage/freddie/historical_data1_Q11999.txt")

df.show(10)


for c,n in zip(df.columns,real_colnames):
    df = df.withColumnRenamed(c,n)

df.show(10)

df.printSchema()

# save_to_postgresql on AWS RDS
#url = "jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/mortgagepgsql"
#df.write \
    #.format("jdbc") \
    #.option("url", url) \
    #.option("driver","org.postgresql.Driver") \
    #.option("dbtable", "freddie_origin") \
    #.option("dbname", "mortgagepgsql") \s
    #.option("user", "sylviaxuinsight") \
    #.option("password", "564210126Aa") \
    #.mode("append")

#def save_to_postgresql(self):
        #"""
        #save batch processing results into PostgreSQL database
        #"""
        #config = {key: self.psql_config[key] for key in
                  ["url", "driver", "user", "password", "mode_batch", "dbtable_batch", "nums_partition"]}
        #self.df.write \
            #.format("jdbc") \
            #.option("url", config["url"]) \
            #.option("driver", config["driver"]) \
            #.option("dbtable", config["dbtable_batch"]) \
            #.option("user", config["user"]) \
            #.option("password", config["password"]) \
            #.mode(config["mode_batch"]) \
            #.option("numPartitions", config["nums_partition"]) \
            #.save()


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
