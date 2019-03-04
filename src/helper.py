#!/usr/bin/python
import os

def read_text_file(url_pattern,real_colnames):
    data = sc.textFile(url_pattern)
    data_1 = data.map(lambda x: x.split('|')).map(lambda x: [i.encode('utf-8') for i in x])
    df = sqlContext.createDataFrame(data_1)
    for c , n in zip(df.columns, real_colnames):
        df = df.withColumnRenamed(c , n)
    return df
