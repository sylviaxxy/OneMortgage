from __future__ import absolute_import, unicode_literals
import os
from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

airflow_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2019, 1, 01, 0, 0),
    'retry_delay': datetime.timedelta(minutes=5) # airflow will run next quarter
}

dag = DAG('onemortgage_monthly',
          default_args=airflow_args,
          schedule_interval= '@monthly')

now = datetime.now()

download_fannie = BashOperator(task_id='download_fannie',
                             bash_command="/home/ubuntu/OneMortgage/src/bash/download_fannie.sh",
                             dag=dag)

download_freddie = BashOperator(task_id='download_freddie',
                             bash_command='/home/ubuntu/OneMortgage/src/bash/download_freddie.sh',
                             dag=dag)

process_data = BashOperator(task_id='process_data',
                            bash_command='/home/ubuntu/OneMortgage/src/bash/run_batch.sh',
                            dag=dag)

process_data.set_upstream([download_freddie, download_fannie])

