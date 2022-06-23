import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def batch_script(input_path, bucket_name, password):
    spark = SparkSession.builder\
            .appName('Batch_Github')\
            .getOrCreate()

    df = spark.read\
        .json(input_path)


    df = df.withColumn('created_at', F.to_timestamp(df.created_at))

    general_table = df.select(['id','created_at','type'])\
                    .withColumnRenamed('actor', 'user')\
                    .withColumnRenamed('type','EventType')\
                    .withColumn('hour', F.hour(df.created_at))


    general_table.createOrReplaceTempView("general_table")

    event_count_table = spark.sql("""
                                SELECT 
                                    EventType,
                                    count(1) AS count
                                from general_table
                                GROUP BY EventType
                                ORDER BY count DESC
                                        """)


    general_table.write\
        .format("io.github.spark_redshift_community.spark.redshift")\
        .option("url", f"jdbc:redshift://dbtredshift.c7adklm4l5cg.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password={password}")\
        .option("dbtable", "general_table")\
        .option("tempdir", f"s3://{bucket_name}/spark_logs")\
        .option("forward_spark_s3_credentials", 'true')\
        .mode("append")\
        .save()

    event_count_table.write\
            .format("io.github.spark_redshift_community.spark.redshift")\
            .option("url", f"jdbc:redshift://dbtredshift.c7adklm4l5cg.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password={password}")\
            .option("dbtable", "event_count_table")\
            .option("tempdir", f"s3://{bucket_name}/spark_logs")\
            .option("forward_spark_s3_credentials", 'true')\
            .mode("append")\
            .save()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',required=True, type=str)
    parser.add_argument('--bucket',required=True, type=str)
    parser.add_argument('--password',required=True, type=str)


    args = parser.parse_args()
    batch_script(args.input, args.bucket, args.password)