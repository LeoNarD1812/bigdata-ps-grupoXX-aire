
import json
import time
import os

from pathlib import Path
from datetime import datetime, timezone

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
    DoubleType
)

# ==========================================
# 1. CONFIGURACION
# ==========================================

PAQUETE = (
    "org.apache.spark:"
    "spark-sql-kafka-0-10_2.13:4.2.0"
)

KAFKA_SERVER = "kafka:9092"
TOPIC = "aeronaves-telemetria"

# Mantener estas rutas para recuperar
# los datos y checkpoints anteriores.
DESTINO = (
    "/workspace/artifacts/s8/"
    "parquet_aeronaves_final"
)

CHECKPOINT = (
    "/workspace/artifacts/checkpoints/"
    "s8_final_v1"
)

# Nuevo archivo de metricas para S9
ARCHIVO_METRICAS = Path(
    "/workspace/artifacts/s9/spark_progress.json"
)


# ==========================================
# 2. INICIAR SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("S8-Pipeline-Final-Aeronaves")
    .master("local[2]")
    .config("spark.jars.packages", PAQUETE)
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("SPARK INICIADO:", spark.version, flush=True)


# ==========================================
# 3. ESQUEMA DE TELEMETRIA
# ==========================================

schema = StructType([
    StructField("eventId", StringType()),
    StructField("eventType", StringType()),
    StructField("schemaVersion", IntegerType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("timePosition", LongType()),
    StructField("eventTime", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("baroAltitude", DoubleType()),
    StructField("velocity", DoubleType()),
    StructField("verticalRate", DoubleType()),
    StructField("source", StringType())
])


# ==========================================
# 4. LEER EVENTOS DESDE KAFKA
# ==========================================

kafka = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_SERVER
    )
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .load()
)


# ==========================================
# 5. TRANSFORMACION Y VALIDACION
# ==========================================

eventos = (
    kafka
    .select(
        col("partition").alias("kafkaPartition"),
        col("offset").alias("kafkaOffset"),
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select(
        "kafkaPartition",
        "kafkaOffset",
        "data.*"
    )
    .withColumn(
        "eventTimestamp",
        to_timestamp("eventTime")
    )

    # Validacion del contrato
    .filter(
        col("eventType") == "aeronave.telemetria"
    )
    .filter(col("schemaVersion") == 1)
    .filter(col("source") == "opensky")
    .filter(col("eventId").isNotNull())
    .filter(
        col("icao24").rlike("^[0-9a-f]{6}$")
    )
    .filter(col("timePosition").isNotNull())
    .filter(col("eventTimestamp").isNotNull())

    # Validacion de coordenadas
    .filter(col("latitude").between(-90, 90))
    .filter(col("longitude").between(-180, 180))

    # Validacion de altitud
    .filter(
        col("baroAltitude").isNull()
        | col("baroAltitude").between(
            -500, 20000
        )
    )

    # Validacion de velocidad
    .filter(
        col("velocity").isNull()
        | col("velocity").between(0, 400)
    )

    # Validacion de velocidad vertical
    .filter(
        col("verticalRate").isNull()
        | col("verticalRate").between(
            -100, 100
        )
    )
)


# ==========================================
# 6. WATERMARK Y DEDUPLICACION
# ==========================================

limpios = (
    eventos
    .withWatermark(
        "eventTimestamp",
        "3 hours"
    )
    .dropDuplicatesWithinWatermark([
        "icao24",
        "timePosition"
    ])
)


# ==========================================
# 7. SELECCIONAR COLUMNAS
# ==========================================

resultado = limpios.select(
    "eventId",
    "icao24",
    "callsign",
    "timePosition",
    "eventTimestamp",
    "latitude",
    "longitude",
    "baroAltitude",
    "velocity",
    "verticalRate",
    "source",
    "kafkaPartition",
    "kafkaOffset"
)


# ==========================================
# 8. FUNCION PARA GUARDAR METRICAS S9
# ==========================================

def guardar_metricas(progreso):
    """
    Guarda estadisticas del ultimo microlote
    de Spark Structured Streaming.

    El exporter de S9 leera este archivo
    para exponerlas a Prometheus.
    """

    ARCHIVO_METRICAS.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    duraciones = progreso.get(
        "durationMs", {}
    ) or {}

    operadores = progreso.get(
        "stateOperators", []
    ) or []

    entradas = progreso.get(
        "numInputRows", 0
    )

    velocidad = progreso.get(
        "processedRowsPerSecond", 0
    )

    duracion = duraciones.get(
        "triggerExecution", 0
    )

    metricas = {
        "batchId": progreso.get("batchId", -1),
        "numInputRows": entradas,
        "inputRowsPerSecond": progreso.get(
            "inputRowsPerSecond", 0
        ),
        "processedRowsPerSecond": velocidad,
        "triggerExecutionMs": duracion,
        "durationMs": duraciones,
        "stateOperators": operadores,
        "updatedAt": datetime.now(
            timezone.utc
        ).timestamp()
    }

    # Escritura atomica: el exporter no
    # leera un JSON parcialmente escrito.
    archivo_temporal = (
        ARCHIVO_METRICAS.with_suffix(".tmp")
    )

    archivo_temporal.write_text(
        json.dumps(
            metricas,
            indent=2,
            default=str
        ),
        encoding="utf-8"
    )

    os.replace(
        archivo_temporal,
        ARCHIVO_METRICAS
    )

    return metricas


# ==========================================
# 9. INICIAR PIPELINE STREAMING
# ==========================================

print("\n====================================")
print("PIPELINE FINAL S8 + METRICAS S9")
print("====================================")
print("KAFKA:", KAFKA_SERVER)
print("TOPIC:", TOPIC)
print("PARQUET:", DESTINO)
print("CHECKPOINT:", CHECKPOINT)
print("METRICAS:", ARCHIVO_METRICAS)
print("====================================\n")

query = (
    resultado.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", DESTINO)
    .option(
        "checkpointLocation",
        CHECKPOINT
    )
    .trigger(
        processingTime="10 seconds"
    )
    .queryName("pipeline_final_s8")
    .start()
)


# ==========================================
# 10. MONITOREAR MICROLOTES
# ==========================================

ultimo_batch = None

try:
    while query.isActive:

        time.sleep(5)

        progreso = query.lastProgress

        if not progreso:
            continue

        batch = progreso.get("batchId")

        # Evitar guardar repetidamente
        # el mismo microlote.
        if batch == ultimo_batch:
            continue

        ultimo_batch = batch

        # Guardar metricas para Prometheus
        metricas = guardar_metricas(progreso)

        print(
            "\n====================================",
            flush=True
        )

        print(
            "MICROLOTE COMPLETADO:",
            batch,
            flush=True
        )

        print(
            "Entradas:",
            metricas["numInputRows"],
            flush=True
        )

        print(
            "Filas procesadas/segundo:",
            metricas[
                "processedRowsPerSecond"
            ],
            flush=True
        )

        print(
            "Duracion del trigger (ms):",
            metricas[
                "triggerExecutionMs"
            ],
            flush=True
        )

        print(
            "Metricas guardadas en:",
            ARCHIVO_METRICAS,
            flush=True
        )

        print(
            "====================================",
            flush=True
        )

        # Mostrar metricas del estado
        for operador in progreso.get(
            "stateOperators", []
        ):
            print(
                "Estado:",
                json.dumps(
                    operador,
                    indent=2,
                    default=str
                ),
                flush=True
            )

except KeyboardInterrupt:

    print(
        "\nDETENIENDO PIPELINE FINAL...",
        flush=True
    )

finally:

    if query.isActive:
        query.stop()

    spark.stop()

    print(
        "SPARK FINALIZADO",
        flush=True
    )
