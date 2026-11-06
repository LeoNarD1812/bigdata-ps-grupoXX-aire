
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

PAQUETE_KAFKA = (
    "org.apache.spark:"
    "spark-sql-kafka-0-10_2.13:4.2.0"
)

spark = (
    SparkSession.builder
    .appName("BigDataU2-PruebaKafka")
    .master("local[2]")
    .config("spark.jars.packages", PAQUETE_KAFKA)
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\nSPARK:", spark.version)

try:
    print("\nCONECTANDO A KAFKA...")

    df = (
        spark.read
        .format("kafka")
        .option(
           "kafka.bootstrap.servers",
           "kafka:9092"
        )
        .option(
            "subscribe",
            "aeronaves-telemetria"
        )
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    resultado = df.select(
        col("key").cast("string").alias("icao24"),
        col("value").cast("string").alias("json"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )

    print("\nTOTAL MENSAJES:", resultado.count())

    print("\nPRIMEROS MENSAJES:")
    resultado.show(5, truncate=80)

    print("\nCONEXION SPARK-KAFKA CORRECTA")

finally:
    spark.stop()
