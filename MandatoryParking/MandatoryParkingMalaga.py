import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

schemetypes = [StructField("PoiID", IntegerType(), True),
               StructField("nombre", StringType(), True),
               StructField("direccion", StringType(), True),
               StructField("telefono", StringType(), True),
               StructField("correoelectronico", StringType(), True),
               StructField("latitude", DoubleType(), True),
               StructField("longitude", DoubleType(), True),
               StructField("altitud", IntegerType(), True),
               StructField("capacidad", IntegerType(), True),
               StructField("capacidad_discapacitados", IntegerType(), True),
               StructField("fechadora_ultima_actualizacion", TimestampType(), True),
               StructField("libres", IntegerType(), True),
               StructField("libres_discapacitados", IntegerType(), True),
               StructField("nivelocupacion_naranja", IntegerType(), True),
               StructField("nivelocupacion_roja", IntegerType(), True),
               StructField("smassa_sector_sare", StringType(), True)]


def main(directory) -> None:
    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("SelectingParkingMalaga") \
        .getOrCreate()

    # Create DataFrame.
    lines = spark \
        .readStream \
        .format("csv") \
        .schema(StructType(schemetypes)) \
        .load(directory)

    lines.printSchema()

    # Compute the average temperature
    values = lines.select("nombre", "capacidad", "libres") \
        .where("capacidad > 0") \


    values.printSchema()

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingAverageTemperatureV2 <directory>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
