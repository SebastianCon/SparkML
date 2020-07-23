import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import window

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
    slots = spark \
        .readStream \
        .format("csv") \
        .schema(StructType(schemetypes)) \
        .load(directory)

    slots.printSchema()

    windows_size = 300
    slide_size = 300

    window_duration = '{} seconds'.format(windows_size)
    slide_duration = '{} seconds'.format(slide_size)

    #Create New Colum and added.

    #new_data_frame = slots.withColumn("ocupados", (slots.capacidad - slots.libres))

    # Compute the average temperature
    values = slots.withColumn("ocupados", (slots.capacidad - slots.libres))\
        .where("capacidad > 0") \
        .groupBy(window(slots.fechadora_ultima_actualizacion, window_duration, slide_duration), slots.nombre, )\
        .agg(functions.mean("ocupados").alias("MeanOcupated")) \
        .orderBy("nombre")

    values.printSchema()

    # Start running the query that prints the output in the screen
    query = values \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate","false") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit StreamingAverageTemperatureV2 <directory>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
