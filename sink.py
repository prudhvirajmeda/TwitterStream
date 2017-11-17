from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession 

if __name__ == "__main__":
 
    
    
    HOST = "localhost"
    PORT = 9092
    KAFKA_SERVER = "{0}:{1}".format(HOST, PORT)
    TOPIC = 'test'

    CHECKPOINT_DIR = "./checkpoint/"
    PARQUET_DIR = "./output/"

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    sc = spark.sparkContext

    sqlcont = SQLContext(sc)

    # Create DataSet representing the stream of input lines from kafka
    # sc = spar.sparkContext
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers",KAFKA_SERVER)\
        .option('subscribe',TOPIC)\
        .option('startingOffsets','latest')\
        .load()\
        .selectExpr("CAST(value AS STRING)")


    
    
    # query = lines\
    #     .writeStream\
    #     .format('console')\
    #     .start()

    query = lines\
        .writeStream\
        .format('parquet')\
        .outputMode('append')\
        .option("checkpointLocation",CHECKPOINT_DIR)\
        .option('path',PARQUET_DIR)\
        .start()

    query.awaitTermination()
