import os
import re
import sys
import boto3
import pyspark
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import *

def pyspark_setting():
    """
    Setup pyspark
    :return:
    """
    conf = pyspark.SparkConf()
    sc = pyspark.SparkContext()
    aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    s3 = boto3.resource('s3')
    sqlContext = pyspark.SQLContext(sc)

def read_text_file(url_pattern, real_colnames):
    """
    Read txt files from urls with defined pattern, and transform rdd into dataframe
    :param url_pattern: a string represents file url pattern
    :param real_colnames: correct column names of the dataframe
    :return spark dataframe with real column names
    """
    data = sc.textFile(url_pattern)
    data_1 = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
    df = sqlContext.createDataFrame(data_1)
    for c, n in zip(df.columns, real_colnames):
        df = df.withColumnRenamed(c, n)
    return df

def freddie_origination_load():
    """
    Read all Freddie Mac origination data
    :return df_freddie_o: dataframe contains of all Freddie Mac origination data
    """
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
    df_freddie_o = read_text_file(freddie_origination_url, freddie_origination_colnames)
    return df_freddie_o

def freddie_performance_load():
    """
    Read all Freddie Mac monthly performance data
    :return df_freddie_p: dataframe contains of all Freddie Mac performance data
    """
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
    """
    Read all Fannie Mae origination data
    :return df_fannie_o: dataframe contains of all Fannie Mae origination data
    """
    fannie_origination_url = 's3a://onemortgage/fannie/Acquisition_*.txt'
    fannie_origination_colnames = ["loan_seq_no",
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
    df_fannie_o = read_text_file(fannie_origination_url, fannie_origination_colnames)
    return df_fannie_o

def fannie_performance_load():
    """
    Read all Fannie Mae monthly performance data
    :return df_fannie_p: dataframe contains of all Fannie Mae performance data
    """
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
    df_fannie_p = read_text_file(fannie_performance_url, fannie_performance_colnames)
    return df_fannie_p

def add_agency_col(df, agency_name):
    """
    Add the column 'agency_id' to a dataframe
    :param df: a dataframe created from raw data
    :param agency_name: should be 'freddie' or 'fannie'
    :return: dataframe with extra column named 'agency_id'
    """
    if agency_name in ["freddie", "fannie"]:
        if agency_name == "freddie":
            df = df.withColumn("agency_id", lit("0"))
        else:
            df = df.withColumn("agency_id", lit("1"))
        return df
    else:
        print("Wrong agency_name!")

def loan_contract_cols():
    """
    Generate a list 'loan_contact_cols'
    :return loan_contract_cols
    """
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
    return loan_contract_cols

def maturity_cols():
    """
    Generate a list 'maturity_cols'
    :return maturity_cols
    """
    maturity_cols = ["loan_seq_no",
                    "first_payment_date",
                    "maturity_date"]
    return maturity_cols

def df_freddie_o_unify(df):
    """
    Unify some freddie mac origination dataframe columns.
    :param df: freddie mac origination dataframe
    :return: cleaned dataframe
    """
    df = df.na.replace(["R", "B", "C", "T", "9"], ["R", "B", "C", "N", "N"], "channel")
    df = df.withColumn("originate_year", F.year(F.to_date(df.first_payment_date, "yyyyMM")))
    df = df.withColumn("originate_month", F.month(F.to_date(df.first_payment_date, "yyyyMM")))
    df = df.withColumn("first_payment_date_formatted",
                       unix_timestamp("first_payment_date", "yyyyMM").cast("double").cast("timestamp"))
    wellfargo_pat = "(?i)\b(?=\w*w)(?=\w*e)(?=\w*l)(?=\w*l)(?=\w*f)(?=\w*a)(?=\w*r)(?=\w*g)(?=\w*o)\w+"
    df = df.withColumn("seller_name", regexp_replace("seller_name", wellfargo_pat, "WELLS FARGO"))
    credit_median = df.approxQuantile("credit_score", [0.5], 0.25) # use approximate quantile to reduce calculation cost
    df = df.withColumn("credit_score").fillna(credit_median)
    return df

def df_fannie_o_unify(df):
    """
    Unify some fannie mae origination dataframe columns.
    :param df: fannie mae origination dataframe
    :return: cleaned dataframe
    """
    df = df.na.replace(["R", "B", "C"], ["R", "B", "C"], "channel")
    df = df.withColumn("originate_year", F.year(F.to_date(df.first_payment_date, "MM/yyyy")))
    df = df.withColumn("originate_month", F.month(F.to_date(df.first_payment_date, "MM/yyyy")))
    df = df.withColumn("first_payment_date_formatted",
                       unix_timestamp("first_payment_date", "MM/yyyy").cast("double").cast("timestamp"))
    df = df.withColumn("seller_name", regexp_replace("seller_name", wellfargo_pat, "WELLS FARGO"))
    credit_median = df.approxQuantile("credit_score", [0.5], 0.25) # use approximate quantile to reduce calculation cost
    df = df.withColumn("credit_score").fillna(credit_median)
    return df

def union_origination(df_freddie,df_fannie):
    """
    Append Freddie and Fannie dataframes together
    :param df_freddie: df with freddie data
    :param df_fannie: df with freddie data
    :return df_loans_temp: df with all the rows in df_freddie and df_fannie

    """
    if df_freddie.columns == df_fannie.columns:
        df_loans_temp = df_freddie.union(df_fannie)
    else:
        print("Two dataframe have different columns, can not append now.")
    return df_loans_temp

def execute_pgsql(pg_config, sql):
    """
    Execute sql query in a postgresql database
    :param pg_config: configuration of target postgresql database
    :param sql: sql query that need to be executed
    :return:
    """
    try:
        conn = psycopg2.connect(database=pg_config['dbname'],
                                user=pg_config['pg_user'],
                                password=pg_config['pg_password'],
                                host=pg_config['pg_host'],
                                port=pg_config['port'])
    except:
        print("Unable to connect to the database!")
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except:
        print("Unable to execute the sql syntax!")
    conn.commit()
    conn.close()
    cur.close()
    print("Operations completed, commit connect and close cursor.")


def schema_transformer_loan_contract(df_loans_temp):
    """
    Cast the schema into correct format
    :param df_loans_temp: df_loans_temp with types of columns require transformation
    :return process df_loans_temp
    """
    numeric_cols_loan_contract = ["original_loan_term",
                                  "original_cltv", "original_dti",
                                  "original_upb", "original_ltv",
                                  "original_interest_rate"]
    for col_name in numeric_cols_loan_contract:
        df_loans_temp = df_loans_temp.withColumn(col_name, col(col_name).cast(DoubleType()))
    return df_loans_temp

def write_table_pgsql(df, jdbc_config):
    """
    Write table to postgresql database
    :param df: dataframe wait writing
    :param jdbc_config: jbbc config dic
    :return:
    """
    df.write.format("jdbc").option("url", jdbc_config['url']). \
        option("driver", jdbc_config['driver']). \
        option("dbtable", jdbc_config['dbtable']). \
        option("dbname", jdbc_config['dbname']). \
        option("user", jdbc_config['user']). \
        option("password", jdbc_config['password']). \
        option("numPartitions", jdbc_config['numPartitions']). \
        mode("overwrite").save()
    print("successfully write the dataframe into pgsqbl table")

def avg_rate_us_by_year(df):
    """
    Summarize df and calculate average interest per year.
    :param df: dataframe of loan orgination
    :return: average original interest year per year dataframe
    """
    avg_rate_us_by_year = df.groupBy("originate_year").\
                            agg(F.mean("original_interest_rate"),F.count("original_interest_rate")).\
                            orderBy('originate_year', ascending=True)
    return avg_rate_us_by_year

def df_freddie_p_unify(df):
    """
    Unify some freddie mac performance dataframe columns.
    :param df: freddie mac performance dataframe
    :return: cleaned dataframe
    """
    df = df.withColumn("report_period_year", F.year(F.to_date(df.report_period, "yyyyMM")))
    df = df.withColumn("report_period_month", F.month(F.to_date(df.report_period, "yyyyMM")))
    return df

def df_fannie_p_unify(df):
    """
    Unify some fannie mae performance dataframe columns.
    :param df: fannie mae performance dataframe
    :return: cleaned dataframe
    """
    df = df.withColumn("report_period_year", F.year(F.to_date(df.report_period, "MM/yyyy")))
    df = df.withColumn("report_period_month", F.month(F.to_date(df.report_period, "MM/yyyy")))
    return df

def schema_transformer_loan_performance(df_loan_performance_temp):
    """
    Transform the type of a df as defined struct
    :param df_loan_performance_temp: dataframe of loan performance which requires transformation
    :return df_loan_
    """
    numeric_cols_loan_performance = ["cur_actual_upb",
                                     "loan_age",
                                     "cur_deferred_upb",
                                     "net_sale_proceeds",
                                     "cur_interest_rate"]
    for col_name in numeric_cols_loan_performance:
        df_loans_p_temp = df_loans_performance_temp.withColumn(col_name, col(col_name).cast(DoubleType()))
    return df_loans_p_temp


def main():
    """
    The main function for ETL job
    """
    pyspark_setting()

    df_freddie_o = freddie_origination_load()
    df_freddie_p = freddie_performance_load()
    df_fannie_o = fannie_origination_load()
    df_fannie_p = fannie_performance_load()

    df_freddie_o_1 = add_agency_col(df_freddie_o, "freddie")
    df_freddie_p_1 = add_agency_col(df_freddie_p, "freddie")
    df_fannie_o_1 = add_agency_col(df_fannie_o, "fannie")
    df_fannie_p_1 = add_agency_col(df_fannie_p, "fannie")

    loan_contract_cols = loan_contract_cols()

    df_freddie_o_temp = df_freddie_o_1.select(*loan_contract_cols)
    df_freddie_o_temp = df_freddie_o_unify(df_freddie_o_temp)
    df_fannie_o_temp = df_fannie_o_1.select(*loan_contract_cols)
    df_fannie_o_temp = df_fannie_o_unify(df_fannie_o_temp)

    df_loans_temp = union_origination(df_freddie_o_temp, df_fannie_o_temp)
    df_loans_save = schema_transformer_loan_contract(df_loans_temp)

    jbdc_config_loan_contract_write = {
        'url': 'jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql',
        'driver': 'org.postgresql.Driver',
        'dbtable': 'loan_contract',
        'dbname': 'pgsql',
        'user': 'sylviaxuinsight',
        'password': '568284947Aa',
        'numPartitions': '10000'}
    write_table_pgsql(df_loans_save, jbdc_config_loan_contract_write)

    jbdc_config_avg_rate_year_write = {
        'url': 'jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql',
        'driver': 'org.postgresql.Driver',
        'dbtable': 'avg_rate_us_by_year',
        'dbname': 'pgsql',
        'user': 'sylviaxuinsight',
        'password': '568284947Aa',
        'numPartitions': '10000'}

    jbdc_config_loan_performance_write = {
        'url': 'jdbc:postgresql://mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com:5432/pgsql',
        'driver': 'org.postgresql.Driver',
        'dbtable': 'loan_performance',
        'dbname': 'pgsql',
        'user': 'sylviaxuinsight',
        'password': '568284947Aa',
        'numPartitions': '10000'}


    pg_config = {'pg_host': 'mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com',
                 'pg_user': 'sylviaxuinsight',
                 'pg_password': '568284947Aa',
                 'dbname': 'pgsql',
                 'port': '5432'}

    sql_create_avg_rate_us_by_year = "CREATE TABLE avg_rate_us_by_year( \
                                      originate_year integer, \
                                      avg_interest_rate numeric, \
                                      loan_count numeric);"

    execute_pgsql(pg_config, sql_create_avg_rate_us_by_year)
    df_avg_rate_us_by_year = avg_rate_us_by_year(df_loans_save)
    write_table_pgsql(df_avg_rate_us_by_year, jbdc_config_avg_rate_year_write)

    loan_performance_cols = ["loan_seq_no",
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
    df_loan_p_temp = df_freddie_p_temp.union(df_fannie_p_temp)
    df_freddie_p_temp = df_freddie_p_unify(df_freddie_p_temp)
    df_fannie_p_temp = df_fannie_p_1.select(*loan_performance_cols)
    df_fannie_p_temp = df_fannie_p_unify(df_fannie_p_temp)
    df_loan_p_save = schema_transformer_loan_contract(df_loan_p_temp)
    write_table_pgsql(df_loan_p_save, jbdc_config_loan_performance_write)
    sc.stop()

if __name__ == '__main__':
    main()

