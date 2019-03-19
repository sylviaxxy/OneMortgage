# OneMortgage
>**A tool to integrate and analyze mortgage datasets in different formats from various sources.**

This is a project for the Insight Data Engineering program (New York, Spring 2019).

[Demo Slideshare](https://www.slideshare.net/SylviaXinyuXu/one-mortgage)

# Introduction

Mortgage is an important method for people to get financed to buy home. Around 78% of houses in US in 2018 were purchased with mortgage. 
In the mortgage market, single family mortgage loans are a type with large total amount. By 2018 Q3, over 2/3 of all holders' outstanding loans in US are single-famliy mortgage loans. 

Hence, accurately and effectively analyze the single-family mortgage market is important for participants in this market.

In this project, we use big data technologies such as S3, Spark, PostgreSQL(on RDS) on AWS to build a tool to ingest, integrate, clean and analyze single family mortgage datasets released by Freddie Mac and Fannie Mae.

# Approach

This project aims at creating a robust data pipeline for integrating the multiple datasets about single-family mortgage loans.

Raw data are published by Freddie Mac and Fannie Mae, which are a large portion of the single-family mortgage loans they acquired.

# Pipeline

![data_pipeline](data_pipeline.png "Data Pipeline")

***Data Download***: python is used to download datasets from corresponding websites 

***File System***: AWS S3 is used to store unzipped txt files

***Batch Processing***: ETL job are done using Spark (pyspark) 

***Data Warehouse***:  Postgre SQL on AWS RDS instance

> The download and etl process is scheduled using Airflow.

### Data Sources

  1. Freddie Mac: [Freddie Mac data](https://freddiemac.embs.com/FLoan/Data/download2.php), for period from 1999 to 2017

  2. Fannie Mae: [Fannie Mae data](https://loanperformancedata.fanniemae.com/lppub/index.html#Portfolio), from 2000 to 2018
 
### Environment Setup

Install and configure [AWS CLI](https://aws.amazon.com/cli/) and [Pegasus](https://github.com/InsightDataScience/pegasus) on your local machine, and clone this repository using
`git clone git@github.com:sylviaxxy/OneMortgage.git`.

> AWS security groups setting: Add your local IP to your AWS VPC inbound rules

> Pegasus: Set up as the guide of Pegasus.

#### CLUSTER STRUCTURE:

To reproduce my environment, 4 m4.large AWS EC2 instances and  1 micro instance are needed:

- Spark Cluster Node (4 m4.large instances) for batch job
- Dash Node (a micro instance qualified for  AWS free-tier)

##### Airflow setup

The Apache Airflow scheduler can be installed on the master node of *spark-batch-cluster*.

##### PostgreSQL setup

The PostgreSQL database sits on AWS RDS instance.

##### Configurations

Configuration settings for PostgreSQL, AWS S3 bucket, as well as the schemas for the data are stored.


## How to start

### Run the Batch Job Once Only
Run `bash/run_batch.sh` on the master of *spark-batch-cluster*  to run the batch job only once.

### If want to use Airflow to schedule the Batch Job 
Run `bash/run_airflow.sh` on the master of *spark-batch-cluster* to add the batch job to the scheduler. The batch job is set to execute every month, and it can be started and monitored using Airflow UI.

### Frontend
After complete the batch processing, run `bash/run_app.sh` to run the Dash app.

## Folder Structure
-
|____requirements.txt
|____data_pipeline.png
|____README.md
|____src
| |____database
| | |____create_schema.sql
| |____app
| | |____app.py
| |____bash
| | |____run_batch.sh
| | |____download_fannie.sh
| | |____download_freddie.sh
| | |____run_app.sh
| | |____run_airflow.sh
| |____Airflow
| | |____airflow_scheduler.py
| |____data_download
| | |____freddie_data_prepare.py
| | |____fannie_data_prepare.py
| |____batch_processing
| | |____elt.py

## Author
Sylvia Xu