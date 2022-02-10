import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
#from pyspark.sql import functions as psf


weather_data_part_1 = 's3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json'
complete_weather_data = 's3a://dataminded-academy-capstone-resources/raw/open_aq'

def get_spark_session():

        conf = SparkConf()
        conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3')
        conf.set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
        conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

        spark = (SparkSession
        .builder
        .config(conf=conf)
        .getOrCreate())

        return spark


def retrieve_and_return_nested_cols(cols):
        nested_cols = []
        other_cols = []
        for column in cols:
                if column[1] == 'struct':
                        nested_cols.append(column[0])
                else:
                        other_cols.append(column[0])
        return nested_cols, other_cols


def flatten_and_drop_nested_cols(df, nested_cols, other_cols):
        new_list = []
        for cols in other_cols:
                new_list.append(cols)
        for n_cols in nested_cols:
                new_list.append(n_cols)
        flattened_df = df.select(*new_list)
        cleaned_df = flattened_df.drop(*nested_cols)
        return cleaned_df




if __name__ == '__main__':

        df = get_spark_session().read.json(weather_data_part_1)
        #df.printSchema()
        df_dtype_cols = flights.dtypes
        
        nested_cols, other_cols = retrieve_and_return_nested_cols(df_dtype_cols)
        cleaned_df = flatten_and_drop_nested_cols(df, nested_cols)
        cleaned_df.show()
        #df.select('*', df[].alias('latitude'), df['coordinates.longitude'].alias('longitude'), df['date.local'].alias(('local'), df['date.utc'].alias('utc'))
    