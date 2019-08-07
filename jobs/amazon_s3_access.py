"""
amazon_s3_access.py
~~~~~~~~~~~~~~~~~~~
This module reads the text file stored in AWS S3 using PySpark.
"""
from __future__ import print_function
import os
import configparser
from pyspark.sql import SparkSession

# AWS related constants
aws_profile = "default"
aws_region = "ap-south-1"
s3_bucket = "bigdata-etl/sample_data.txt"

# Reading environment variables from aws credential file
config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))

access_id = config.get(aws_profile, "aws_access_key_id")
access_key = config.get(aws_profile, "aws_secret_access_key")

# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.7 pyspark-shell"
os.environ['PYSPARK_SUBMIT_ARGS'] = "--jars=/DEV/Jars/org.apache.hadoop_hadoop-aws-2.7.7.jar,/DEV/Jars/com.amazonaws_aws-java-sdk-1.7.4.jar pyspark-shell"

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("AmazonS3Access") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    sc = spark.sparkContext
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    hadoop_conf.set("fs.s3a.connection.maximum", "100000")
    hadoop_conf.set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

    dataS3 = spark.read.text("s3a://" + s3_bucket)
    dataS3.show(truncate=False)

    spark.stop()