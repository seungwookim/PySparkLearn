
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf()
conf.setMaster('spark://8ea172cae00f:7077')
conf.setAppName('sessiontest')
conf.set('spark.driver.cores', '1')
conf.set('spark.driver.memory' , '1G')

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)
df_writer = sqlContext.createDataFrame([{'name': 'Andy', 'eng': '800', 'grade': 'A', 'gender': 'female', 'age': '50', 'org': '1', 'univ': 'SKKU'}]).write
df_writer.parquet("hdfs://587ed1df9441:9000/tensormsa/test.parquet " , mode="append", partitionBy=None)
