
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    countDistinct,
    avg,
    min as spark_min,
    max as spark_max,
    round as spark_round,
    col
)

RUTA = "/workspace/artifacts/s8/parquet_aeronaves_final"

spark = (
    SparkSession.builder
    .appName("S8-Verificar-Parquet")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(RUTA)

    print("\n=== ESQUEMA PARQUET ===")
    df.printSchema()

    print("\n=== PRIMERAS OBSERVACIONES ===")
    df.orderBy("eventTimestamp").show(
        10, truncate=False
    )

    print("\n=== INDICADORES GENERALES ===")

    df.agg(
        count("*").alias("totalObservaciones"),
        countDistinct("icao24").alias(
            "aeronavesUnicas"
        ),
        countDistinct("eventId").alias(
            "eventosUnicos"
        ),
        spark_round(
            avg("baroAltitude"), 2
        ).alias("altitudPromedio"),
        spark_round(
            avg("velocity"), 2
        ).alias("velocidadPromedio"),
        spark_min("eventTimestamp").alias(
            "primeraObservacion"
        ),
        spark_max("eventTimestamp").alias(
            "ultimaObservacion"
        )
    ).show(truncate=False)

    print("\n=== CONTROL DE CALIDAD ===")

    total_invalidos = df.filter(
        (col("velocity") > 400) |
        (col("velocity") < 0) |
        (col("latitude") < -90) |
        (col("latitude") > 90) |
        (col("longitude") < -180) |
        (col("longitude") > 180)
    ).count()

    print(
        "Registros fuera de rango:",
        total_invalidos
    )

    print("\n=== POSIBLES EVENTOS DUPLICADOS ===")

    duplicados = (
        df.groupBy("eventId")
        .count()
        .filter(col("count") > 1)
    )

    print(
        "IDs de evento repetidos:",
        duplicados.count()
    )

    duplicados.show(10, truncate=False)


    print("\n=== DUPLICADOS POR AERONAVE Y TIEMPO ===")

    duplicados_observacion = (
        df.groupBy("icao24", "timePosition")
        .count()
        .filter(col("count") > 1)
    )

    cantidad = duplicados_observacion.count()

    print("Combinaciones duplicadas:", cantidad)
    duplicados_observacion.show(20, truncate=False)

    print("\n=== TOTAL DE OBSERVACIONES ===")
    print("Registros Parquet:", df.count())


finally:
    spark.stop()
