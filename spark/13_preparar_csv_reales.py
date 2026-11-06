"""S10: prepara todas las trayectorias CSV de Zenodo en Parquet usando PySpark.
Ejecutar dentro de aire-pyspark con el directorio de entrada accesible en /workspace.
No sintetiza ni altera los valores numéricos originales.
"""
import os
from pyspark.sql import SparkSession, functions as F, types as T

ENTRADA = os.environ.get('CSV_ENTRADA', '/workspace/datasets/historicos/zenodo')
SALIDA = os.environ.get('PARQUET_SALIDA', '/workspace/artifacts/s10/parquet_zenodo_trayectorias')

spark = (SparkSession.builder.appName('S10-Zenodo-CSV-Parquet')
         .master('local[2]')
         .config('spark.sql.session.timeZone', 'UTC')
         .config('spark.sql.shuffle.partitions', '4')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

# Esquema consistente con la cabecera observada en los CSV adjuntos por el usuario.
schema = T.StructType([
    T.StructField('TIME', T.StringType()),
    T.StructField('UTC_TIME', T.StringType()),
    T.StructField('LAT', T.StringType()),
    T.StructField('LON', T.StringType()),
    T.StructField('GEOALTITUDE', T.StringType()),
    T.StructField('VELOCITY', T.StringType()),
    T.StructField('VERTRATE', T.StringType()),
    T.StructField('HEADING', T.StringType()),
    T.StructField('CALLSIGN', T.StringType()),
])
try:
    print('ENTRADA:', ENTRADA, flush=True)
    print('SALIDA:', SALIDA, flush=True)
    raw = (spark.read.format('csv')
           .option('header', 'true')
           .option('recursiveFileLookup', 'true')
           .option('pathGlobFilter', '*.csv')
           .option('mode', 'PERMISSIVE')
           .option('enforceSchema', 'false')
           .schema(schema)
           .load(ENTRADA))

    raw = raw.withColumn('rutaArchivo', F.col('_metadata.file_path'))
    # Nombre de archivo como ID de trayectoria, NO como identificador ICAO real.
    raw = raw.withColumn('trayectoriaId', F.regexp_extract('rutaArchivo', r'([^/\\]+)\.csv$', 1))
    df = (raw
          .withColumn('epochSeconds', F.col('TIME').cast('long'))
          .withColumn('eventTimestamp', F.to_timestamp(F.from_unixtime(F.col('epochSeconds'))))
          .withColumn('latitude', F.col('LAT').cast('double'))
          .withColumn('longitude', F.col('LON').cast('double'))
          .withColumn('geoAltitude', F.col('GEOALTITUDE').cast('double'))
          .withColumn('velocity', F.col('VELOCITY').cast('double'))
          .withColumn('verticalRate', F.col('VERTRATE').cast('double'))
          .withColumn('trueTrack', F.col('HEADING').cast('double'))
          .withColumn('callsign', F.trim(F.col('CALLSIGN')))
          .withColumn('sourceDataset', F.lit('zenodo_csv'))
          .withColumn('fecha', F.to_date('eventTimestamp'))
          .select('trayectoriaId', 'epochSeconds', 'eventTimestamp', 'fecha',
                  'latitude', 'longitude', 'geoAltitude', 'velocity',
                  'verticalRate', 'trueTrack', 'callsign', 'sourceDataset', 'rutaArchivo'))
    total = df.count()
    validos = df.filter(
        F.col('epochSeconds').isNotNull() & F.col('eventTimestamp').isNotNull()
        & F.col('latitude').between(-90, 90)
        & F.col('longitude').between(-180, 180)
        & F.col('velocity').between(0, 400)
        & F.col('trayectoriaId').isNotNull()
        & (F.length('trayectoriaId') > 0)
    ).dropDuplicates(['trayectoriaId', 'epochSeconds'])
    n_validos = validos.count()
    print('FILAS CSV:', total, 'VALIDAS Y NO DUPLICADAS:', n_validos, flush=True)
    validos.agg(F.countDistinct('trayectoriaId').alias('trayectorias'),
                F.countDistinct('fecha').alias('dias'),
                F.min('eventTimestamp').alias('inicio'),
                F.max('eventTimestamp').alias('fin'),
                F.min('latitude').alias('latMin'),
                F.max('latitude').alias('latMax'),
                F.min('longitude').alias('lonMin'),
                F.max('longitude').alias('lonMax')).show(truncate=False)
    print('MUESTRA:')
    validos.select('trayectoriaId', 'eventTimestamp', 'latitude', 'longitude', 'velocity').show(8, truncate=False)
    if n_validos == 0:
        raise RuntimeError('No hay filas válidas; revisar formato de CSV y unidades.')
    # Útil para archivos medianos: particionar por fecha para no crear miles de carpetas.
    validos.repartition(4).write.mode('overwrite').partitionBy('fecha').parquet(SALIDA)
    print('PARQUET GENERADO:', SALIDA, flush=True)
    print('NOTA: trayectoriaId proviene del nombre del archivo; no es ICAO24.', flush=True)
finally:
    spark.stop()
