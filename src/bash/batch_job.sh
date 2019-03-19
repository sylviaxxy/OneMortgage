#!/bin/bash

FOLDER=$PWD
cd /usr/local/spark/bin/

spark-submit --master spark://ip-10-0-0-4:7077 \
             --packages org.postgresql:postgresql:9.4.1212 \
             --driver-memory 4G \
             --executor-memory 4G \
             $FOLDER/etl.py $1