
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType
)

# CONFIGURACION
KAFKA_SERVER = "kafka:9092"
TOPIC = "aeronaves-telemetria"

PAQUETE_KAFKA = (
    "org.apache.spark:"
    "spark-sql-kafka-0-10_2.13:4.2.0"
)

CHECKPOINT = (
    "/workspace/artifacts/checkpoints/"
    "s8_lectura_aeronaves"
)

# 1. INICIAR SPARK
spark = (
    SparkSession.builder
    .appName("S8-Streaming-Aeronaves")
    .master("local[2]")
    .config("spark.jars.packages", PAQUETE_KAFKA)
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("SPARK INICIADO:", spark.version)

# 2. DEFINIR ESQUEMA DE OPENSKY
schema = StructType([
    StructField("eventId", StringType()),
    StructField("eventType", StringType()),
    StructField("schemaVersion", IntegerType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("originCountry", StringType()),
    StructField("timePosition", LongType()),
    StructField("eventTime", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("baroAltitude", DoubleType()),
    StructField("onGround", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("trueTrack", DoubleType()),
    StructField("verticalRate", DoubleType()),
    StructField("geoAltitude", DoubleType()),
    StructField("positionSource", IntegerType()),
    StructField("observedAt", StringType()),
    StructField("source", StringType())
])

# 3. LEER KAFKA EN STREAMING
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4. TRANSFORMAR JSON EN COLUMNAS
aeronaves = (
    kafka_stream
    .select(
        col("key").cast("string").alias("kafkaKey"),
        col("value").cast("string").alias("json"),
        col("partition").alias("kafkaPartition"),
        col("offset").alias("kafkaOffset")
    )
    .withColumn(
        "data",
        from_json(col("json"), schema)
    )
    .select(
        "kafkaKey",
        "kafkaPartition",
        "kafkaOffset",
        "data.*"
    )
    .withColumn(
        "eventTimestamp",
        to_timestamp(col("eventTime"))
    )
)

# 5. FILTRAR EVENTOS VALIDOS
aeronaves_validas = (
    aeronaves
    .filter(col("eventType") == "aeronave.telemetria")
    .filter(col("schemaVersion") == 1)
    .filter(col("source") == "opensky")
    .filter(col("icao24").isNotNull())
    .filter(col("eventTimestamp").isNotNull())
    .filter(col("latitude").between(-90, 90))
    .filter(col("longitude").between(-180, 180))
)

# 6. SELECCIONAR COLUMNAS
resultado = aeronaves_validas.select(
    "icao24",
    "callsign",
    "eventTimestamp",
    "latitude",
    "longitude",
    "baroAltitude",
    "velocity",
    "verticalRate",
    "kafkaPartition",
    "kafkaOffset"
)

# 7. INICIAR STREAMING
print("INICIANDO STREAMING AEREO...")

query = (
    resultado.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("DETENIENDO STREAMING...")
finally:
    query.stop()
    spark.stop()
