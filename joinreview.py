import csv
import re
import math
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import pyspark

'''
Run
spark-submit --master yarn --num-executors 35 --executor-memory 16g --executor-cores 8 joinreview.py
'''
sc = SparkContext(appName="si671")
sqlContext = SQLContext(sc)
df1=spark.read.json('hdfs://cavium-thunderx/user/shaoranz/Inspection_La.json')
df2=spark.read.json('hdfs://cavium-thunderx/user/shaoranz/review.json')
df3=df1['business_id','stars']
df5=df3.groupby(df3.business_id).avg()
df=df2.join(df5,'business_id')
df.write.format('json').save('hdfs://cavium-thunderx/user/shaoranz/review_nv.json')