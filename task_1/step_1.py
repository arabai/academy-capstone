import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as psf
from step_2 import get_secrets
import json

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
                if 'struct' in column[1] :
                        nested_cols.append(column[0])
                else:
                        other_cols.append(column[0])
        return nested_cols, other_cols


def flatten_and_drop_nested_cols(df, nested_cols, other_cols):
        flattened_df = df.select(*other_cols, *[c + ".*" for c in nested_cols])
        cleaned_df = flattened_df.drop(*nested_cols)
        return cleaned_df


def cast_col_to_timestamp(df, cols):
        for col in cols:
                df = df.withColumn(col, psf.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ssXXX"))
        return df

def write_to_snowflake(df):
    credentials = get_secrets()
    secret_string = credentials["SecretString"]

    secrets = json.loads(secret_string)

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    sfOptions = {
        "sfURL" : secrets["URL"] + ".snowflakecomputing.com",
        "sfUser" : secrets["USER_NAME"],
        "sfPassword": secrets["PASSWORD"],
        "sfRole": secrets["ROLE"],
        "sfDatabase" : secrets["DATABASE"],
        "sfSchema" : "ANAS",
        "sfWarehouse" : secrets["WAREHOUSE"]
    }

    (df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("dbtable", "ANAS")
        .mode("overwrite")
        .save())




if __name__ == '__main__':

        df = get_spark_session().read.json(weather_data_part_1)
        df_dtype_cols = df.dtypes
        nested_cols, other_cols = retrieve_and_return_nested_cols(df_dtype_cols)
        cleaned_df = flatten_and_drop_nested_cols(df, nested_cols, other_cols)
        #cleaned_df.printSchema()
        to_cast_cols = cleaned_df.columns[-2:]
        #print(to_cast_cols)
        casted_df = cast_col_to_timestamp(cleaned_df, to_cast_cols)
        casted_df.printSchema()
        write_to_snowflake(casted_df)