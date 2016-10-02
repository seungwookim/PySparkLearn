# -*- coding: utf-8 -*-
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

def save_csv_to_df(data_frame, table_name, csv_file):
    """
    save csv as parquet with pandas
    :param net_id:
    :return:
    """
    conf = SparkConf()
    conf.setMaster('spark://8ea172cae00f:7077')
    conf.setAppName('sessiontest')
    conf.set('spark.driver.cores', '1')
    conf.set('spark.driver.memory', '1G')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='false').load('/home/dev/PySparkLearn/SalesJan2009.csv')
    df.write.parquet("hdfs://587ed1df9441:9000/tensormsa/dataframe/{0}/{1}".format(data_frame, table_name), mode="append", partitionBy=None)

    print(df)


#model_conf = open('/home/dev/PySparkLearn/SalesJan2009.csv', 'r')
save_csv_to_df("test1", "csvtest", '/home/dev/PySparkLearn/SalesJan2009.csv')