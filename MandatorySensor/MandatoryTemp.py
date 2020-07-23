import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import window


def main(directory) -> None:

    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("StreamingFindHighTemperatureWithTimeWindows") \
        .getOrCreate()

    fields = [StructField("sensor", StringType(), True),
              StructField("value", DoubleType(), True),
              StructField("timestamp", TimestampType(), True)]

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("csv") \
        .options(header='false') \
        .schema(StructType(fields)) \
        .load(directory)

    lines.printSchema()

    windows_size = 20
    slide_size = 20

    window_duration = '{} seconds'.format(windows_size)
    slide_duration = '{} seconds'.format(slide_size)

    # Compute the average temperature

    values = lines.groupBy(
        window(lines.timestamp, window_duration, slide_duration), lines.sensor,)\
        .agg(functions.max("value").alias("max")) \
        .orderBy("sensor", "window")

    # values = lines.groupBy(lines.sensor).agg(functions.mean("value").alias("mean"))

    values.printSchema()

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("complete") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingFindHighTemperatureWithTimeWindows <directory>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
