
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    window, count, avg, round as spark_round,
    approx_count_distinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("S8-Ventanas-Aereas")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


schema = StructType([
    StructField("eventId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("schemaVersion", IntegerType(), True),
    StructField("icao24", StringType(), True),
    StructField("eventTime", StringType(), True),

    # Coordenadas de la aeronave
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),

    # Datos de vuelo
    StructField("baroAltitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("verticalRate", DoubleType(), True),

    # Procedencia
    StructField("source", StringType(), True)
])


kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "aeronaves-telemetria")
    .option("startingOffsets", "earliest")
    .load()
)


eventos = (
    kafka
    .select(
        from_json(
            col("value").cast("string"), schema
        ).alias("data")
    )
    .select("data.*")
    .withColumn(
        "eventTimestamp",
        to_timestamp("eventTime")
    )

    # Validacion del contrato
    .filter(col("eventType") == "aeronave.telemetria")
    .filter(col("schemaVersion") == 1)
    .filter(col("source") == "opensky")
    .filter(col("icao24").rlike("^[0-9a-f]{6}$"))
    .filter(col("eventId").isNotNull())
    .filter(col("eventTimestamp").isNotNull())

    # Validacion de coordenadas
    .filter(col("latitude").between(-90, 90))
    .filter(col("longitude").between(-180, 180))

    # Validacion de altitud
    .filter(
        col("baroAltitude").isNull() |
        col("baroAltitude").between(-500, 20000)
    )

    # Validacion de velocidad
    .filter(
        col("velocity").isNull() |
        col("velocity").between(0, 400)
    )

    # Validacion de velocidad vertical
    .filter(
        col("verticalRate").isNull() |
        col("verticalRate").between(-100, 100)
    )
)


# VENTANAS FIJAS: 5 MINUTOS
ventanas_fijas = (
    eventos
    .groupBy(
        window("eventTimestamp", "5 minutes")
    )
    .agg(
        count("*").alias("totalObservaciones"),
        approx_count_distinct("icao24").alias(
            "aeronavesUnicasEstimadas"
        ),
        spark_round(avg("baroAltitude"), 2).alias(
            "altitudPromedio"
        ),
        spark_round(avg("velocity"), 2).alias(
            "velocidadPromedio"
        )
    )
    .select(
        col("window.start").alias("inicio"),
        col("window.end").alias("fin"),
        "totalObservaciones",
        "aeronavesUnicasEstimadas",
        "altitudPromedio",
        "velocidadPromedio"
    )
)

# VENTANAS DESLIZANTES: 5 MINUTOS, CADA 1 MINUTO
ventanas_deslizantes = (
    eventos
    .groupBy(
        window(
            "eventTimestamp",
            "5 minutes",
            "1 minute"
        )
    )
    .agg(
        count("*").alias("totalObservaciones"),
        approx_count_distinct("icao24").alias(
            "aeronavesUnicasEstimadas"
        ),
        spark_round(avg("verticalRate"), 2).alias(
            "velocidadVerticalPromedio"
        )
    )
    .select(
        col("window.start").alias("inicio"),
        col("window.end").alias("fin"),
        "totalObservaciones",
        "aeronavesUnicasEstimadas",
        "velocidadVerticalPromedio"
    )
)

print("INICIANDO VENTANAS FIJAS Y DESLIZANTES")

query_fija = None
query_deslizante = None

try:
    query_fija = (
        ventanas_fijas.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 30)
        .option(
            "checkpointLocation",
            "/workspace/artifacts/checkpoints/"
            "s8_ventanas_fijas_v2"
        )
        .trigger(processingTime="15 seconds")
        .queryName("ventanas_fijas")
        .start()
    )

    query_deslizante = (
        ventanas_deslizantes.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 30)
        .option(
            "checkpointLocation",
            "/workspace/artifacts/checkpoints/"
            "s8_ventanas_deslizantes_v2"
        )
        .trigger(processingTime="15 seconds")
        .queryName("ventanas_deslizantes")
        .start()
    )

    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    print("DETENIENDO CONSULTAS")

finally:
    if query_fija is not None:
        query_fija.stop()
    if query_deslizante is not None:
        query_deslizante.stop()
    spark.stop()
