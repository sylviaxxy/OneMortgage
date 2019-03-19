#!/usr/bin/env bash

# Run on master
# Register the dag
python ~/OneMortgage/src/airflow/airflow_scheduler.py

# Initialize airflow
airflow webserver -p 8081 &
airflow scheduler &