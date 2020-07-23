# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import pandas as pd
from hashlib import sha1
import urllib.request
from socket import timeout

if __name__ == '__main__':
    # Create a SparkConf object
    spark_session = SparkSession \
        .builder \
        .master("local[4]") \
        .config("spark.network.timeout", "2000s") \
        .config("spark.executor.heartbeatInterval", "1000s") \
        .getOrCreate()

    # Configure loggers
    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Read csv and create a set of pseudonyms to de_anonymize
    df = pd.read_csv("D:\salary-anon.comma.csv",  # CSV file
                     header=None,  # The CSV has no header row
                     names=["PSEUDONYM", "SALARY"],  # The name of columns for the dataframe
                     index_col=0)  # Set the first column as index.

    search_set = set(df.index)  # The set of sha1 values from the dataframe.


    def brute_force(dni):
        sha = sha1(str(dni).encode()).hexdigest()
        return sha if sha in search_set else None


    udf_brute_force = f.udf(brute_force, StringType())

    bf = spark_session.range(100000, 1000000000)
    bf = bf.withColumn('SHA', udf_brute_force(bf['id']))
    # bf = bf.filter(bf.id==142478)
    # bf = bf.na.drop(subset=["SHA"])
    bf = bf.where(f.col("SHA").isNotNull())
    bf.show()





