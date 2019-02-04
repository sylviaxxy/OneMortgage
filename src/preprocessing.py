# PySpark code to process raw data
# pyspark --packages org.postgresql:postgresql:9.4.1212 --master spark://ip-10-0-0-4:7077
# spark-submit /home/ubuntu/OneMortgage/src/helper.py --packages org.postgresql:postgresql:9.4.1212 --master spark://ip-10-0-0-4:7077
import os
import re
import boto3
import pyspark
import psycopg2
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import helper

conf = pyspark.SparkConf()
sc = pyspark.SparkContext()
#hadoop_conf = sc._jsc.hadoopConfiguration()
#hadoop_conf.set("fs.s3n.awsAccessKeyId", aws_id)
s3 = boto3.resource('s3')
sqlContext = pyspark.SQLContext(sc)

#aws_id = os.environ.get('AWS_ACCESS_KEY_ID')
#aws_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


# Process Freddie Mac data
def read_text_file(url_pattern,real_colnames):
    data = sc.textFile(url_pattern)
    data_1 = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
    df = sqlContext.createDataFrame(data_1)
    for c,n in zip(df.columns, real_colnames):
        df = df.withColumnRenamed(c , n)
    return df

freddie_origination_url = 's3a://onemortgage/freddie/historical_data1_Q*.txt'
freddie_origination_colnames = ['credit_score',
                                'first_payment_date',
                                'first_time_homebuyer_flag',
                                'maturity_date',
                                'msa',
                                'mip',
                                'num_units',
                                'occupancy_status' ,
                                'original_cltv',
                                'original_dti',
                                'original_upb',
                                'original_ltv',
                                'original_interest_rate',
                                'channel',
                                'prepayment_penalty_flag',
                                'product_type',
                                'property_state',
                                'property_type',
                                'postal_code',
                                'loan_seq_no',
                                'loan_purpose',
                                'ori_term',
                                'num_borrower',
                                'seller_name',
                                'servicer_name',
                                'super_conforming_flag']

freddie_performance_url = sc.textFile('s3a://onemortgage/freddie/historical_data1_time_Q*.txt')
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
                                "dti",
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

fannie_performance_url = 's3a://onemortgage/fannie/Performance_*.tx'
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

df_freddie_o = read_text_file(freddie_origination_url,freddie_origination_colnames)
df_freddie_p = read_text_file(freddie_performance_url, freddie_performance_colnames)
df_fannie_o = read_text_file(fannie_origination_url,fannie_origination_colnames)
df_fannie_p = read_text_file(fannie_performance_url,fannie_performance_colnames)
