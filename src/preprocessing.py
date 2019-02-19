# PySpark code to process raw data
# pyspark --packages org.postgresql:postgresql:9.4.1212 --master spark://ip-10-0-0-4:7077
# spark-submit /home/ubuntu/OneMortgage/src/helper.py --packages org.postgresql:postgresql:9.4.1212 --master spark://ip-10-0-0-4:7077
import sys
sys.path.append("./helpers/")

import os
import re
import boto3
import pyspark
import psycopg2
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import helper

def pyspark_setting():
    conf = pyspark.SparkConf()
    sc = pyspark.SparkContext()
    #hadoop_conf = sc._jsc.hadoopConfiguration()
    #hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
    #aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
    #aws_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    s3 = boto3.resource('s3')
    sqlContext = pyspark.SQLContext(sc)

pyspark_setting()


# Process Freddie Mac data
def read_text_file(url_pattern,real_colnames):
    data = sc.textFile(url_pattern)
    data_1 = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
    df = sqlContext.createDataFrame(data_1)
    for c,n in zip(df.columns, real_colnames):
        df = df.withColumnRenamed(c , n)
    return df

def freddie_origination_load():
    freddie_origination_url = 's3a://onemortgage/freddie/historical_data1_Q*.txt'
    freddie_origination_colnames = ["credit_score",
                                    "first_payment_date",
                                    "first_time_homebuyer_flag",
                                    "maturity_date",
                                    "msa",
                                    "mip",
                                    "num_of_units",
                                    "occupancy_status",
                                    "original_cltv",
                                    "original_dti",
                                    "original_upb",
                                    "original_ltv",
                                    "original_interest_rate",
                                    "channel",
                                    "prepayment_penalty_flag",
                                    "product_type",
                                    "property_state",
                                    "property_type",
                                    "postal_code",
                                    "loan_seq_no",
                                    "loan_purpose",
                                    "original_loan_term",
                                    "number_of_borrowers",
                                    "seller_name",
                                    "servicer_name",
                                    "super_conforming_flag"]
    df_freddie_o = read_text_file(freddie_origination_url,freddie_origination_colnames)
    return df_freddie_o

def freddie_performance_load():
    freddie_performance_url = 's3a://onemortgage/freddie/historical_data1_time_Q*.txt'
    freddie_performance_colnames = ["loan_seq_no",
                                    "report_period",
                                    "cur_actual_upb",
                                    "cur_delinquency",
                                    "loan_age",
                                    "mon_to_maturity",
                                    "repurchase",
                                    "modification",
                                    "zero_balance_code",
                                    "zero_balance_date",
                                    "cur_interest_rate",
                                    "cur_deferred_upb",
                                    "ddlpi",
                                    "mi_recoveries",
                                    "net_sale_proceeds",
                                    "non_mi_recoveries",
                                    "expenses",
                                    "legal_costs",
                                    "maintain_costs",
                                    "tax_insurance",
                                    "miscellaneous_expenses",
                                    "actual_loss",
                                    "modification_cost",
                                    "step_modification",
                                    "deferred_payment_modification",
                                    "estimated_ltv"]
    df_freddie_p = read_text_file(freddie_performance_url, freddie_performance_colnames)
    return df_freddie_p

def fannie_origination_load():
    fannie_origination_url = 's3a://onemortgage/fannie/Acquisition_*.txt'
    fannie_origination_colnames= ["loan_seq_no",
                                    "channel",
                                    "seller_name",
                                    "original_interest_rate",
                                    "original_upb",
                                    "original_loan_term",
                                    "origination_date",
                                    "first_payment_date",
                                    "original_ltv",
                                    "original_cltv",
                                    "number_of_borrowers",
                                    "original_dti",
                                    "credit_score",
                                    "first_time_homebuyer_flag",
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
    df_fannie_o = read_text_file(fannie_origination_url,fannie_origination_colnames)
    return df_fannie_o

def fannie_performance_load():
    fannie_performance_url = 's3a://onemortgage/fannie/Performance_*.txt'
    fannie_performance_colnames = ["loan_seq_no",
                                  "report_period",
                                  "servicer_name",
                                  "cur_interest_rate",
                                  "cur_actual_upb",
                                  "loan_age",
                                  "mon_to_maturity",
                                  "adjusted_mon_to_maturity",
                                  "maturity_date",
                                  "msa",
                                  "cur_delinquency",
                                  "modification",
                                  "zero_balance_code",
                                  "zero_balance_date",
                                  "last_paid_installment_date",
                                  "foreclosure_date",
                                  "disposition_date",
                                  "foreclosure_costs",
                                  "property_preservation_repair_costs",
                                  "asset_recovery_costs",
                                  "miscellaneous_expenses",
                                  "associated_taxes",
                                  "net_sale_proceeds",
                                  "credit_enhancement_proceeds",
                                  "repurchase_make_whole_proceeds",
                                  "other_foreclousure_proceeds",
                                  "non_interest_bearing_upb",
                                  "principal_forgiveness_amount",
                                  "repurchase_make_whole_proceeds_flag",
                                  "foreclousure_principle_write_off_amount",
                                  "servicing_activity_indicator"]
    df_fannie_p = read_text_file(fannie_performance_url,fannie_performance_colnames)
    return df_fannie_p

def add_agency_col(df,agency_name):
    if agency_name in ["freddie", "fannie"]:
        if agency_name == "freddie":
            df = df.withColumn("agency_id", lit("0"))
        else:
            df= df.withColumn("agency_id", lit("1"))
        return df
    else:
        print("Wrong agency_name!")

df_freddie_o = freddie_origination_load()
df_freddie_p = freddie_performance_load()
df_fannie_o = fannie_origination_load()
df_fannie_p = fannie_performance_load()

df_freddie_o_1 = add_agency_col(df_freddie_o,"freddie")
df_freddie_p_1 = add_agency_col(df_freddie_p,"freddie")
df_fannie_o_1 = add_agency_col(df_fannie_o, "fannie")
df_fannie_p_1 = add_agency_col(df_fannie_p, "fannie")


def create_loan_contract(df_freddie_o, df_freddie_p, df_fannie_o, df_fannie_p):
    loan_contract_cols = ["loan_seq_no",
                            "agency_id",
                            "first_payment_date",
                            "first_time_homebuyer_flag",
                            "maturity_date",
                            "msa",
                            "mip",
                            "original_loan_term",
                            "original_cltv",
                            "original_dti",
                            "original_upb",
                            "original_ltv",
                            "original_interest_rate",
                            "channel",
                            "prepayment_penalty_flag",
                            "product_type",
                            "loan_purpose",
                            "super_conforming_flag",
                            "hpi_index_id",
                            "final_zero_balance_code",
                            "sato",
                            "mortgage_insurance_type",
                            "relocation_mortgage_indicator",
                            "vintage",
                            "seller_id",
                            "servicer_id"]

    loans_from_freddie_o =["loan_seq_no",
                            "agency_id",
                            "first_payment_date",
                            "first_time_homebuyer_flag",
                            "maturity_date",
                            "msa",
                            "mip",
                            "original_loan_term",
                            "original_cltv",
                            "original_dti",
                            "original_upb",
                            "original_ltv",
                            "original_interest_rate",
                            "channel",
                            "prepayment_penalty_flag",
                            "product_type",
                            "loan_purpose",
                            "super_conforming_flag"]

    loans_from_fannie_o = ["loan_seq_no",
                           "agency_id",
                            "first_payment_date",
                            "first_time_homebuyer_flag",
                            "mip",
                            "original_loan_term",
                            "original_cltv",
                            "original_dti",
                            "original_upb",
                            "original_ltv",
                            "original_interest_rate",
                            "channel",
                            "product_type",
                            "loan_purpose",
                            "relocation_mortgage_indicator"]

    loans_from_fannie_p = ["loan_seq_no",
                            "msa",
                            "maturity_date"]
    df_freddie_o_temp = df_freddie_o_1.select(*loans_from_freddie_o)
    df_freddie_loan_temp = df_freddie_o_temp\
                            .join(df_freddie_p_temp, df_freddie_p_temp.loan_seq_no == \
                            df_freddie_o_temp.loan_seq_no, "left")
    df_fannie_o_temp = df_fannie_o_1.select(*loans_from_fannie_o)
    df_fannie_p_temp =  df_fannie_p_1.select(*loans_from_fannie_p)
    df_fannie_p_temp_1 = df_fannie_p_temp_1.groupBy("log_seq_no").agg(first("msa"))
    df_loans = df_freddie_loan_temp.union(de_fannie_o_temp)

    df_fannie_p_temp2 = df.select(*loans_from_fannie_p).filter(df['msa']. \
                            isNotNull()).groupBy(df_fannie_o_temp2["loan_seq_no"]).\
                            agg(F.first(df_fannie_p_temp2["msa"]),
                            F.first(df_fannie_p_temp2["maturity_date"]))).

def missing_handling_loan_contract():

################# New Version ####################
import os
import re
import boto3
import pyspark
import psycopg2
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession

conf = pyspark.SparkConf()
conf.setMaster("spark://ip-10-0-0-4:7077")
sc = pyspark.SparkContext()
#hadoop_conf = sc._jsc.hadoopConfiguration()
#hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
s3 = boto3.resource('s3')
sqlContext = pyspark.SQLContext(sc)

loan_contract_schema =StructType([
                    StructField("credit_score", IntegerType(), True),
                    StructField("first_payment_date", StringType(), True),
                    StructField("first_time_homebuyer_flag", StringType(), True),
                    StructField("maturity_date", DateType(), True),
                    StructField("msa", StringType(), True),
                    StructField("mip", Numericype(), True),
                    StructField("num_of_units", NumericType(), True),
                    StructField("occupancy_status", StringType(), True),
                    StructField("original_cltv", NumericType(), True),
                    StructField("original_dti", NumericType(), True),
                    StructField("original_upb", NumericType(), True),
                    StructField("original_ltv", NumericType(), True),
                    StructField("original_interest_rate", NumericType(), True),
                    StructField("channel", StringType(), True),
                    StructField("prepayment_penalty_flag", StringType(), True),
                    StructField("product_type", StringType(), True),
                    StructField("property_state", StringType(), True),
                    StructField("property_type", StringType(), True),
                    StructField("postal_code", StringType(), True),
                    StructField("loan_seq_no", StringType(), True),
                    StructField("loan_purpose", StringType(), True),
                    StructField("original_loan_term", NumericType(), True),
                    StructField("number_of_borrowers", NumericType(), True),
                    StructField("seller_name", StringType(), True),
                    StructField("servicer_name", StringType(), True),
                    StructField("super_conforming_flag", StringType(), True)])

def read_text_file(url_pattern,real_colnames):
    data = sc.textFile(url_pattern)
    data_1 = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
    df = sqlContext.createDataFrame(data_1)
    schema = {}
    for c,n in zip(df.columns, real_colnames):
        df = df.withColumnRenamed(c , n)
    return df

def freddie_origination_load():
    freddie_origination_url = 's3a://onemortgage/freddie/historical_data1_Q*.txt'
    freddie_origination_colnames = ["credit_score",
                                    "first_payment_date",
                                    "first_time_homebuyer_flag",
                                    "maturity_date",
                                    "msa",
                                    "mip",
                                    "num_of_units",
                                    "occupancy_status",
                                    "original_cltv",
                                    "original_dti",
                                    "original_upb",
                                    "original_ltv",
                                    "original_interest_rate",
                                    "channel",
                                    "prepayment_penalty_flag",
                                    "product_type",
                                    "property_state",
                                    "property_type",
                                    "postal_code",
                                    "loan_seq_no",
                                    "loan_purpose",
                                    "original_loan_term",
                                    "number_of_borrowers",
                                    "seller_name",
                                    "servicer_name",
                                    "super_conforming_flag"]
    df_freddie_o = read_text_file(freddie_origination_url,freddie_origination_colnames)
    return df_freddie_o

def freddie_performance_load():
    freddie_performance_url = 's3a://onemortgage/freddie/historical_data1_time_Q*.txt'
    freddie_performance_colnames = ["loan_seq_no",
                                    "report_period",
                                    "cur_actual_upb",
                                    "cur_delinquency",
                                    "loan_age",
                                    "mon_to_maturity",
                                    "repurchase",
                                    "modification",
                                    "zero_balance_code",
                                    "zero_balance_date",
                                    "cur_interest_rate",
                                    "cur_deferred_upb",
                                    "ddlpi",
                                    "mi_recoveries",
                                    "net_sale_proceeds",
                                    "non_mi_recoveries",
                                    "expenses",
                                    "legal_costs",
                                    "maintain_costs",
                                    "tax_insurance",
                                    "miscellaneous_expenses",
                                    "actual_loss",
                                    "modification_cost",
                                    "step_modification",
                                    "deferred_payment_modification",
                                    "estimated_ltv"]
    df_freddie_p = read_text_file(freddie_performance_url, freddie_performance_colnames)
    return df_freddie_p

def fannie_origination_load():
    fannie_origination_url = 's3a://onemortgage/fannie/Acquisition_*.txt'
    fannie_origination_colnames= ["loan_seq_no",
                                    "channel",
                                    "seller_name",
                                    "original_interest_rate",
                                    "original_upb",
                                    "original_loan_term",
                                    "origination_date",
                                    "first_payment_date",
                                    "original_ltv",
                                    "original_cltv",
                                    "number_of_borrowers",
                                    "original_dti",
                                    "credit_score",
                                    "first_time_homebuyer_flag",
                                    "loan_purpose",
                                    "property_type",
                                    "number_of_units",
                                    "occupancy_status",
                                    "property_state",
                                    "postal_code",
                                    "mip",
                                    "product_type",
                                    "co_borrower_credit_score",
                                    "mortgage_insurance_type",
                                    "relocation_mortgage_indicator"]
    df_fannie_o = read_text_file(fannie_origination_url,fannie_origination_colnames)
    return df_fannie_o

def fannie_performance_load():
    fannie_performance_url = 's3a://onemortgage/fannie/Performance_*.txt'
    fannie_performance_colnames = ["loan_seq_no",
                                  "report_period",
                                  "servicer_name",
                                  "cur_interest_rate",
                                  "cur_actual_upb",
                                  "loan_age",
                                  "mon_to_maturity",
                                  "adjusted_mon_to_maturity",
                                  "maturity_date",
                                  "msa",
                                  "cur_delinquency",
                                  "modification",
                                  "zero_balance_code",
                                  "zero_balance_date",
                                  "last_paid_installment_date",
                                  "foreclosure_date",
                                  "disposition_date",
                                  "foreclosure_costs",
                                  "property_preservation_repair_costs",
                                  "asset_recovery_costs",
                                  "miscellaneous_expenses",
                                  "associated_taxes",
                                  "net_sale_proceeds",
                                  "credit_enhancement_proceeds",
                                  "repurchase_make_whole_proceeds",
                                  "other_foreclousure_proceeds",
                                  "non_interest_bearing_upb",
                                  "principal_forgiveness_amount",
                                  "repurchase_make_whole_proceeds_flag",
                                  "foreclousure_principle_write_off_amount",
                                  "servicing_activity_indicator"]
    df_fannie_p = read_text_file(fannie_performance_url,fannie_performance_colnames)
    return df_fannie_p

def add_agency_col(df,agency_name):
    if agency_name in ["freddie", "fannie"]:
            if agency_name == "freddie":
                df = df.withColumn("agency_id", lit("0"))
            else:
                df= df.withColumn("agency_id", lit("1"))
            return df
    else:
        print("Wrong agency_name!")

df_freddie_o = freddie_origination_load()
df_freddie_p = freddie_performance_load()
df_fannie_o = fannie_origination_load()
df_fannie_p = fannie_performance_load()

df_freddie_o_1 = add_agency_col(df_freddie_o,"freddie")
df_freddie_p_1 = add_agency_col(df_freddie_p,"freddie")
df_fannie_o_1 = add_agency_col(df_fannie_o, "fannie")
df_fannie_p_1 = add_agency_col(df_fannie_p, "fannie")

loan_contract_cols = ["loan_seq_no",
                       "agency_id",
                        "credit_score",
                        "first_payment_date",
                        "first_time_homebuyer_flag",
                        "original_loan_term",
                        "original_cltv",
                        "original_dti",
                        "original_upb",
                        "original_ltv",
                        "original_interest_rate",
                        "channel",
                        "property_state",
                        "loan_purpose",
                        "seller_name"]

maturity_cols = ["loan_seq_no",
                 "first_payment_date",
                "maturity_date"]

df_freddie_o_temp = df_freddie_o_1.select(*loan_contract_cols)

#import org.apache.spark.sql.functions.{dayofmonth,from_unixtime,month, unix_timestamp, year}

def df_freddie_o_unify(df):
    df = df.na.replace(["R","B","C","T","9"],["R","B","C","N","N"],"channel")
    df = df.withColumn("originate_year", F.year(F.to_date(df.first_payment_date, "yyyyMM")))
    df = df.withColumn("originate_month", F.month(F.to_date(df.first_payment_date, "yyyyMM")))
    df = df.withColumn("first_payment_date_formatted", unix_timestamp("first_payment_date","yyyyMM").cast("double").cast("timestamp"))
    return df

df_freddie_o_temp = df_freddie_o_unify(df_freddie_o_temp)

df_fannie_o_temp = df_fannie_o_1.select(*loan_contract_cols)
def df_fannie_o_unify(df):
    df = df.na.replace(["R","B","C"],["R","B","C"],"channel")
    df = df.withColumn("originate_year", F.year(F.to_date(df.first_payment_date, "MM/yyyy")))
    df = df.withColumn("originate_month", F.month(F.to_date(df.first_payment_date, "MM/yyyy")))
    df = df.withColumn("first_payment_date_formatted", unix_timestamp("first_payment_date","MM/yyyy").cast("double").cast("timestamp"))
    return df

df_fannie_o_temp = df_fannie_o_unify(df_fannie_o_temp)

def missing_checker_channel(df):
    df_fannie_o_temp.filter((df_fannie_o_temp["channel"] == "") | df_fannie_o_temp["channel"].isNull() | isnan(df_fannie_o_temp["channel"])).count()
    return df_missing_result

##from pyspark.sql.functions import col
##F.rowNumber().over(Window.partitionBy("loan_seq_no").orderBy(col("unit_count").desc())

def cheker_before_union(df1,df2):
    assert df_freddie_o_temp.columns == df_fannie_o_temp.columns

df_loans_temp = df_freddie_o_temp.union(df_fannie_o_temp)

sql_create_loan_contract = "CREATE TABLE loan_contract( \
    loan_seq_no text, \
    agency_id text, \
    credit_score integer, \
    first_payment_date date, \
    first_time_homebuyer_flag text, \
    original_loan_term numeric, \
    original_cltv numeric, \
    original_dti numeric, \
    original_upb numeric, \
    original_ltv numeric, \
    original_interest_rate numeric, \
    channel text, \
    property_state text, \
    loan_purpose text, \
    seller_name text, \
    originate_year integer, \
    originate_month integer, \
    first_payment_timestamp timestamp);"

pg_config ={'pg_host':'mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com',
            'pg_user':'sylviaxuinsight',
            'pg_password':'568284947Aa',
            'dbname':'pgsql',
            'port':'5432'}

def execute_pgsql(pg_config,sql):
    try:
        conn = psycopg2.connect(database = pg_config['dbname'], \
                                user = pg_config['pg_user'], \
                                password = pg_config['pg_password'],\
                                host = pg_config['pg_host'], \
                                port = pg_config['port'])
    except:
        print("unable to connect to the database!")
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except:
        print("unable to execute the sql syntax!")
    conn.commit()
    conn.close()
    cur.close()
    print("operations completed, commit connect and close cursor")

execute_pgsql(pg_config,sql_create_loan_contract)

def schema_transformer_loan_contract(df_loans_temp):
    """Transform the type of a df as defined struct"""
    # Cast the schema into correct format
    numeric_cols_loan_contract = ["original_loan_term",
                                  "original_cltv","original_dti",
                                  "original_upb","original_ltv",
                                  "original_interest_rate"]
    for col_name in numeric_cols_loan_contract:
        df_loans_temp = df_loans_temp.withColumn(col_name,col(col_name).cast(DoubleType()))
    return df_loans_temp


df_loans_save = schema_transformer_loan_contract(df_loans_temp)

df_loans_save.printSchema()

jbdc_config_loan_contract_write = {
                'url':'jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql',
                'driver': 'org.postgresql.Driver',
                'dbtable':'loan_contract',
                'dbname': 'pgsql',
                'user': 'sylviaxuinsight',
                'password': '568284947Aa',
                'numPartitions':'10000'}

def write_table_pgsql(df,jdbc_config):
    df.write.format("jdbc").option("url", jdbc_config['url']). \
                option("driver",jdbc_config['driver']). \
                option("dbtable", jdbc_config['dbtable']). \
                option("dbname", jdbc_config['dbname']). \
                option("user", jdbc_config['user']). \
                option("password", jdbc_config['password']). \
                option("numPartitions", jdbc_config['numPartitions']). \
                mode("overwrite").save()
    print("successfully write the dataframe into pgsqbl table")


sql_create_avg_rate_us_by_year="CREATE TABLE avg_rate_us_by_year( \
    originate_year integer, \
    avg_interest_rate numeric, \
    count numeric);"

execute_pgsql(pg_config,sql_create_avg_rate_us_by_year)
#write table 'loan_contract' into postgresql
write_table_pgsql(df_loans_save,jbdc_config_loan_contract_write)


def avg_rate_us_by_year(df):
    avg_rate_us_by_year = df.groupBy("originate_year").agg(F.mean("original_interest_rate"), F.count("original_interest_rate"))

avg_rate_us_by_year = df_loans_save.groupBy("originate_year").agg(F.mean("original_interest_rate"), \
                        F.count("original_interest_rate")).orderBy('originate_year', ascending=True)

jbdc_config_loan_contract_write = {
                'url':'jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql',
                'driver': 'org.postgresql.Driver',
                'dbtable':'avg_rate_us_by_year',
                'dbname': 'pgsql',
                'user': 'sylviaxuinsight',
                'password': '568284947Aa',
                'numPartitions':'10000'}
write_table_pgsql(avg_rate_us_by_year,jbdc_config_loan_contract_write)

################# TO DO ##########################
loan_performance_cols =  ["loan_seq_no",
                        "agency_id"
                        "report_period",
                        "cur_actual_upb",
                        "cur_deferred_upb",
                        "cur_delinquency",
                        "cur_interest_rate",
                        "zero_balance_code",
                        "zero_balance_date",
                        "mon_to_maturity",
                        "net_sale_proceeds",
                        "loan_age"]

df_freddie_p_temp = df_freddie_p_1.select(*loan_performance_cols)

def df_freddie_p_unify(df):
    #df = df.na.replace(["R","B","C","T","9"],["R","B","C","N","N"],"channel")
    df = df.withColumn("report_period_year", F.year(F.to_date(df.report_period, "yyyyMM")))
    df = df.withColumn("report_period_month", F.month(F.to_date(df.report_period, "yyyyMM")))
    return df

df_freddie_p_temp = df_freddie_p_unify(df_freddie_p_temp)

df_fannie_p_temp = df_fannie_p_1.select(*loan_performance_cols)
def df_fannie_p_unify(df):
    #df = df.na.replace(["R","B","C"],["R","B","C"],"channel")
    df = df.withColumn("report_period_year", F.year(F.to_date(df.report_period, "MM/yyyy")))
    df = df.withColumn("report_period_month", F.month(F.to_date(df.report_period, "MM/yyyy")))
    return df

df_fannie_p_temp = df_fannie_p_unify(df_fannie_p_temp)

df_freddie_p_temp.columns == df_fannie_p_temp.columns

df_loan_p_temp = df_freddie_p_temp.union(df_fannie_p_temp)

def schema_transformer_loan_performance(df_loan_performance_temp):
    """Transform the type of a df as defined struct"""
    # Cast the schema into correct format
    numeric_cols_loan_contract = ["original_loan_term",
                                  "original_cltv","original_dti",
                                  "original_upb","original_ltv",
                                  "original_interest_rate"]
    for col_name in numeric_cols_loan_contract:
        df_loans_temp = df_loans_temp.withColumn(col_name,col(col_name).cast(DoubleType()))
    return df_loans_temp

df_loan_p_save = schema_transformer_loan_contract(df_loan_p_temp)
df_loan_p_save.printSchema()

sql_create_loan_performance = "CREATE TABLE loan_performance( \
                    loan_seq_no text, \
                    agency_id text, \
                    report_period date, \
                    cur_actual_upb numeric, \
                    cur_deferred_upb numeric, \
                    cur_delinquency text, \
                    cur_interest_rate numeric , \
                    zero_balance_code text, \
                    zero_balance_date date, \
                    mon_to_maturity integer, \
                    net_sale_proceeds numeric, \
                    loan_age integer, \
                    report_period_year integer, \
                    report_period_month integer);"

execute_pgsql(pg_config,sql_create_loan_performance)

jbdc_config_loan_performance_write = {
                'url':'jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql',
                'driver': 'org.postgresql.Driver',
                'dbtable':'loan_performance',
                'dbname': 'pgsql',
                'user': 'sylviaxuinsight',
                'password': '568284947Aa',
                'numPartitions':'10000'}

write_table_pgsql(df_loan_p_save,jbdc_config_loan_performance_write)

property_cols=["loan_seq_no",
                "number_of_units",
                "occupancy_status",
                "property_state",
                "postal_code"]
df_property =

borrower_cols =["loan_seq_no",
                "credit_score",
                "number_of_borrowers",
                "co_borrower_credit_score"]
df_borrower =

hpi_cols = ["hpi_date",
            "hpi_index"]

hpi_url = 's3a://onemortgage/index/CSUSHPINSA.csv'

lines = sc.textFile(hpi_url)
parts = lines.map(lambda l: l.split(','))
df_hpi = spark.createDataFrame(parts, ['hpi_date','hpi_index'])

census = ["state",
        "population",
        "year"]

df_census = spar.createDataFrame(,[])
