import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as psf
from pyspark.streaming import StreamingContext
import time

# TODO Create a schema for incoming resources
schema = StructType([
        StructField("crime_id",LongType()),
        StructField("original_crime_type_name",StringType()),
        StructField("report_date",DateType()),
        StructField("call_date",DateType()),
        StructField("offense_date",DateType()),
        StructField("call_time",TimestampType()),
        StructField("call_date_time",DateType()),
        StructField("disposition",StringType()),
        StructField("address",StringType()),
        StructField("city",StringType()),
        StructField("state",StringType()),
        StructField("agency_id",IntegerType()),
        StructField("address_type",StringType()),
        StructField("common_location",StringType())
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9093") \
        .option("subscribe","com.udacity.project.sfcrimepolice") \
        .option("startingOffset","earliest") \
        .option("maxOffsetsPerTrigger",200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    

    # Show schema for the incoming resources for checks
 
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr( "cast (value as string)  value")
   
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    # TODO select original_crime_type_name and disposition
    
    service_table.createOrReplaceTempView("service_table")
    distinct_table = spark.sql("select original_crime_type_name, disposition,call_time \
       FROM service_table ").distinct()
   


    agg_df =  distinct_table.selectExpr(" original_crime_type_name","call_time").withWatermark("call_time", "10 minutes").groupBy("original_crime_type_name",window("call_time","10 minutes","5 minutes")).count()
    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    #agg_df.filter(" status == 'Processed'")
    query = agg_df.writeStream.outputMode("append").format("console").start()
            
    time.sleep(8)
       
    
    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = ".\radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df,agg_df.disposition==radio_code_df.disposition )


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port",3000)\
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")
    ssc = StreamingContext(spark, 1)
   
    run_spark_job(spark)

    spark.stop()
