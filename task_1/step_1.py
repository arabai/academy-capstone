import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F

weather_data = 's3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json'

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3')
conf.set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')



if __name__ == '__main__':
    spark = (SparkSession
                .builder
                .config(conf=conf)
                .getOrCreate())

    weather_df = spark.read.json(weather_data)
    weather_df.show()
    weather_df.printSchema()

    
    