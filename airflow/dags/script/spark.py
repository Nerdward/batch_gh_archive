import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def batch_script(input_path):
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
        .option("url", "jdbc:redshift://dbtredshift.c7adklm4l5cg.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Neohakim2000")\
        .option("dbtable", "general_table")\
        .option("tempdir", "s3://nerdward-bucket/spark_logs")\
        .option("forward_spark_s3_credentials", 'true')\
        .mode("append")\
        .save()

    event_count_table.write\
            .format("io.github.spark_redshift_community.spark.redshift")\
            .option("url", "jdbc:redshift://dbtredshift.c7adklm4l5cg.us-east-1.redshift.amazonaws.com:5439/dev?user=awsuser&password=Neohakim2000")\
            .option("dbtable", "event_count_table")\
            .option("tempdir", "s3://nerdward-bucket/spark_logs")\
            .option("forward_spark_s3_credentials", 'true')\
            .mode("append")\
            .save()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',required=True, type=str)

    args = parser.parse_args()
    batch_script(args.input)