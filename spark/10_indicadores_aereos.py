
import json
import os
import time

from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    countDistinct,
    avg,
    round as spark_round,
    max as spark_max
)

# ======================================
# CONFIGURACION
# ======================================

RUTA_PARQUET = (
    "/workspace/artifacts/s8/"
    "parquet_aeronaves_final"
)

ARCHIVO_SALIDA = Path(
    "/workspace/artifacts/s9/"
    "indicadores_aereos.json"
)

INTERVALO = 30

# ======================================
# INICIAR SPARK
# ======================================

spark = (
    SparkSession.builder
    .appName("S9-Indicadores-Aeronaves")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("INDICADORES AEREOS INICIADOS", flush=True)

# ======================================
# CALCULAR INDICADORES
# ======================================

def calcular_indicadores():

    df = spark.read.parquet(RUTA_PARQUET)

    resumen = df.agg(
        count("*").alias("observaciones"),
        countDistinct("icao24").alias("aeronaves"),
        spark_round(
            avg("baroAltitude"), 2
        ).alias("altitud"),
        spark_round(
            avg("velocity"), 2
        ).alias("velocidad"),
        spark_max("eventTimestamp").alias(
            "ultimo_evento"
        )
    ).first()

    datos = {
        "totalObservaciones": resumen["observaciones"],
        "aeronavesUnicas": resumen["aeronaves"],
        "altitudPromedio": resumen["altitud"],
        "velocidadPromedio": resumen["velocidad"],
        "ultimoEvento": (
            resumen["ultimo_evento"].isoformat()
            if resumen["ultimo_evento"] else None
        ),
        "updatedAt": datetime.now(
            timezone.utc
        ).timestamp()
    }

    ARCHIVO_SALIDA.parent.mkdir(
        parents=True, exist_ok=True
    )

    temporal = ARCHIVO_SALIDA.with_suffix(".tmp")

    temporal.write_text(
        json.dumps(datos, indent=2),
        encoding="utf-8"
    )

    os.replace(temporal, ARCHIVO_SALIDA)

    print("\nINDICADORES ACTUALIZADOS")
    print(json.dumps(datos, indent=2), flush=True)


# ======================================
# ACTUALIZACION PERIODICA
# ======================================

try:
    while True:
        try:
            calcular_indicadores()
        except Exception as error:
            print("Error leyendo Parquet:", error, flush=True)

        time.sleep(INTERVALO)

except KeyboardInterrupt:
    print("\nINDICADORES DETENIDOS")

finally:
    spark.stop()
