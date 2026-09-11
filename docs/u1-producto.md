# Big Data — Producto de Unidad 1

**Pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML**

---

## Integrantes

| Integrante | Dimensión U1 | Modelo |
| :--- | :--- | :--- |
| Leonardo Jesús Huamán Arhuata | Retraso de vuelo (regresión) | LinearRegression + RandomForestRegressor |
| Joel Jhonathan Quispe Mamani | Congestión de aeropuerto (clasificación) | LogisticRegression + RandomForestClassifier |

---

## 1. Arquitectura Big Data seleccionada

**Arquitectura elegida: Lambda**

El caso de negocio (predicción de retrasos y congestión aérea) requiere tanto análisis histórico como procesamiento en tiempo real, por lo que se selecciona Lambda sobre Kappa.

| Criterio | Respuesta |
| :--- | :--- |
| ¿Necesita histórico? | Sí — 7,079,081 vuelos de 2024 para entrenar modelos |
| ¿Necesita tiempo real? | Sí — estado actual del espacio aéreo para predicción en vivo (U2) |
| Decisión | **Lambda** (batch layer + speed layer + serving layer) |

### Diagrama

```
┌─────────────────────┐
│    Fuente de datos   │
│    Kaggle 2024        │
│    7M vuelos           │
└──────────┬──────────┘
           │
     ┌─────┴─────┐
     │           │
┌────▼────┐ ┌───▼────┐
│  Batch   │ │  Speed  │
│  Layer   │ │  Layer  │
│  (U1)    │ │  (U2)   │
│ PySpark  │ │ Kafka+  │
│          │ │ Spark   │
└────┬────┘ └───┬────┘
     │           │
┌────▼───────────▼────┐
│    Serving Layer      │
│  Notebooks (U1)        │
│  Grafana (U2)          │
└─────────────────────┘
```

### Justificación

- **Batch layer:** necesaria para procesar 7M de registros históricos, entrenar modelos de regresión y clasificación con Spark MLlib, y generar salidas analíticas en Parquet.
- **Speed layer:** se implementará en Unidad 2 con la API de OpenSky Network para inferencia en tiempo real.
- **Serving layer:** en U1 son los notebooks Jupyter con las métricas; en U2 será un tablero Grafana compartido.

---

## 2. Transformaciones distribuidas con PySpark

### 2.1 Leonardo — Retrasos de vuelo

**Extracción con esquema explícito:**

```python
schema_vuelos = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    # ... 35 columnas en total
    StructField("dep_delay", DoubleType(), True),
    StructField("distance", DoubleType(), True),
])

df = spark.read.csv(
    f"{ORIGEN_DATOS}/flight_data_2024.csv",
    header=True,
    schema=schema_vuelos,
)
```

**Transformaciones aplicadas:**

```python
df_prep = (
    df_q1
    .withColumn("fecha", to_date(col("fl_date"), "yyyy-MM-dd"))
    .withColumn("crs_dep_hour", (col("crs_dep_time").cast("int") / 100).cast("int"))
    .withColumn("dow", dayofweek(col("fecha")))
)
```

**Resultado:**

- Total de filas cargadas: 7,079,081
- Filas del primer bimestre 2024 (Q1 parcial): 1,066,492
- Después de limpieza de nulos y cancelaciones: 1,043,101
- Después de deduplicación: 1,042,839

### 2.2 Joel — Congestión de aeropuerto

**Agregación por aeropuerto + hora + fecha:**

```python
df_agg = (
    df_prep
    .groupBy("origin", "fecha", "crs_dep_hour", "dow", "month")
    .agg(
        count("*").alias("vuelos_por_hora"),
        avg("dep_delay").alias("delay_promedio"),
        avg("distance").alias("distancia_promedio"),
    )
)
```

**Resultado:**

- Filas Q1 (3 meses): 1,658,259
- Registros agregados por aeropuerto+hora: 249,148
- Después de deduplicación y limpieza: 246,929

**Derivación de la variable objetivo:**

```python
percentiles = df_agg.approxQuantile("vuelos_por_hora", [0.33, 0.66], 0.01)
umbral_bajo, umbral_alto = percentiles[0], percentiles[1]
# Umbral bajo (P33): 1.0
# Umbral alto (P66): 5.0

df_agg = df_agg.withColumn(
    "congestion_level",
    when(col("vuelos_por_hora") <= umbral_bajo, 0.0)
    .when(col("vuelos_por_hora") <= umbral_alto, 1.0)
    .otherwise(2.0)
)
```

**Distribución resultante:**

- Nivel 0 (Baja): 90,452 registros
- Nivel 1 (Media): 84,281 registros
- Nivel 2 (Alta): 74,415 registros

---

## 3. Calidad de datos y particionamiento analítico

### 3.1 Controles de calidad aplicados

| Control | Técnica | Resultado |
| :--- | :--- | :--- |
| Esquema explícito | StructType con 35 campos | Sin inferencia — tipos controlados |
| Filtro de nulos | `.filter(isNotNull())` | Leonardo: 23,391 filas eliminadas |
| Deduplicación | `.dropDuplicates([...])` | Leonardo: 262 duplicados; Joel: 2,219 duplicados |
| Filtro de cancelaciones | `.filter(col("cancelled") == 0)` | Solo vuelos efectivos |

### 3.2 Particionamiento en Parquet

**Leonardo:**

```python
df_clean.write.mode("overwrite") \
    .partitionBy("month") \
    .parquet(f"{ARTIFACTS}/vuelos_silver")
```

- Estructura de carpetas: `vuelos_silver/month=1/`, `month=2/`
- Verificación de ida y vuelta: OK (1,042,839 filas sin pérdida)

**Joel:**

```python
df_clean.write.mode("overwrite") \
    .partitionBy("month") \
    .parquet(f"{ARTIFACTS}/congestion_silver")
```

- Estructura de carpetas: `congestion_silver/month=1/`, `month=2/`, `month=3/`
- Verificación de ida y vuelta: OK (246,929 filas sin pérdida)

### 3.3 Verificación con PartitionFilters

Al filtrar por la columna de partición, el plan de ejecución (`explain(True)`) muestra:

```
PartitionFilters: [isnotnull(month#...), (month#... = 1)]
```

Esto confirma que Spark evita abrir carpetas que no contienen el mes buscado — optimización conocida como *partition pruning*.

---

## 4. Componente ML distribuido

### 4.1 Leonardo — Regresión de retrasos

**Preparación del vector de predictores:**

```python
categoricas = ["op_unique_carrier", "origin", "dest"]
numericas = ["crs_dep_hour", "dow", "distance", "crs_elapsed_time", "month"]

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx") for c in categoricas]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_oh") for c in categoricas]

predictores = numericas + [f"{c}_oh" for c in categoricas]
assembler = VectorAssembler(inputCols=predictores, outputCol="features")
```

**Split:**

- Entrenamiento: 834,588 filas (80%)
- Prueba: 208,251 filas (20%)

**Modelos entrenados:**

| Modelo | RMSE | R² | MAE |
| :--- | :--- | :--- | :--- |
| Sin regularización | 56.4575 | 0.0176 | 22.9645 |
| Ridge (L2) | 56.4582 | 0.0176 | 22.9649 |
| Elastic Net (L1+L2) | 56.4580 | 0.0176 | 22.9573 |
| 🏆 Random Forest | 56.4404 | 0.0182 | 22.9204 |

**Ganador: Random Forest**, guardado en `artifacts/modelo_retrasos_rf`.

### 4.2 Joel — Clasificación de congestión

**Split:**

- Entrenamiento: 197,788 filas (80%)
- Prueba: 49,141 filas (20%)

**Modelos entrenados:**

| Modelo | Accuracy | F1 |
| :--- | :--- | :--- |
| LogisticRegression | 0.8065 | 0.8075 |
| 🏆 Random Forest | 0.8400 | 0.8393 |

**Top features (Random Forest):**

| Índice | Importancia | Variable (según orden) |
| :--- | :--- | :--- |
| Feature[3] | 0.1706 | delay_promedio |
| Feature[2] | 0.1455 | vuelos_por_hora |
| Feature[4] | 0.0962 | distancia_promedio |
| Feature[12] | 0.0708 | origin (one-hot) |
| Feature[26] | 0.0561 | origin (one-hot) |

**Ganador: Random Forest**, guardado en `artifacts/modelo_congestion_ganador`.

---

## 5. Reflexiones técnicas

### 5.1 Leonardo — ¿Por qué el R² es bajo?

El R² de 0.0182 significa que el modelo explica solo el 1.8% de la varianza de `dep_delay`. Esto es normal y esperado porque:

- Los predictores usados (ruta, aerolínea, hora, distancia) no capturan los factores que realmente causan retrasos: clima en tiempo real, congestión aérea, problemas mecánicos.
- `dep_delay` tiene outliers extremos (hasta 3,777 minutos) que inflan el RMSE.
- El dataset original tiene una desviación estándar de 56.06 — el RMSE del modelo (56.44) apenas la supera.

**¿Por qué Random Forest ganó?** Porque la relación entre predictores y `dep_delay` no es lineal. Random Forest captura interacciones no lineales que LinearRegression no puede representar.

**Aprendizaje clave:** para mejorar este modelo se necesitarían variables adicionales (clima, congestión previa, historial de la aeronave) o cambiar el enfoque a series de tiempo (S10).

### 5.2 Joel — ¿Por qué F1 y no solo accuracy?

Las clases en el dataset de congestión están balanceadas (33% cada una), así que Accuracy y F1 son casi idénticos. Sin embargo, se reportan ambas porque:

- Accuracy puede ser engañosa en problemas con clases desbalanceadas.
- F1 combina precision y recall, penalizando los errores en las clases minoritarias.

**¿Por qué Random Forest ganó?** La congestión depende de patrones complejos: la relación entre hora del día, día de la semana y cantidad de vuelos no es lineal. Random Forest captura esas interacciones naturalmente.

**Feature más importante:** `delay_promedio` con 0.17 de importancia — tiene sentido porque un aeropuerto congestionado acumula retrasos.

### 5.3 Reflexión conjunta — ¿Por qué comparar múltiples modelos?

Como ilustra el caso de Zillow (S4): confiar en un único modelo sin comparación es una decisión de riesgo. En nuestro caso:

- En ambos notebooks, Random Forest superó a los modelos lineales.
- La diferencia en Leonardo es mínima (0.03% en RMSE), lo que sugiere que el problema no es de algoritmo sino de features.
- La diferencia en Joel es significativa (4% en F1), lo que confirma que la congestión tiene patrones no lineales.

Sin la comparación, no habríamos sabido que el problema de Leonardo está en los predictores, no en el modelo.

---

## 6. Evidencias técnicas

Capturas de los notebooks de sustentación S05, organizadas por integrante:

### Leonardo — Regresión de retrasos

**printSchema() del dataset:**

> ⚠ *Captura pendiente — se subió con el mismo nombre de archivo que la de Joel (`01_print_schema.png`) y se sobrescribió. Vuélvela a adjuntar con un nombre distinto (p. ej. `01_print_schema_leonardo.png`) y se incorpora aquí.*

**Conteo después de limpieza (filas antes/después, duplicados):**

![Leonardo: conteo de filas antes/después de limpieza y deduplicación](img/02_df_clean_count.png)
*Figura — Leonardo: conteo de filas antes/después de limpieza y deduplicación*

**Verificación de escritura/lectura Parquet (ida y vuelta):**

![Leonardo: verificación de ida y vuelta sobre vuelos_silver](img/03_partition_filters.png)
*Figura — Leonardo: verificación de ida y vuelta sobre vuelos_silver*

**explain(True) con PartitionFilters:**

![Leonardo: plan de ejecución con PartitionFilters](img/04_tabla_comparativa.png)
*Figura — Leonardo: plan de ejecución con PartitionFilters (partition pruning)*

**Random Forest ganador + modelo guardado:**

![Leonardo: tabla comparativa de modelos y Random Forest ganador guardado](img/05_random_forest_ganador.png)
*Figura — Leonardo: tabla comparativa de modelos y Random Forest ganador guardado*

### Joel — Clasificación de congestión

**printSchema() del dataset:**

![Joel: esquema explícito y conteo total de filas cargadas](img/01_print_schema.png)
*Figura — Joel: esquema explícito y conteo total de filas cargadas*

**Agregación por aeropuerto + hora:**

![Joel: agregación por aeropuerto, fecha y hora programada](img/02_df_agg_show.png)
*Figura — Joel: agregación por aeropuerto, fecha y hora programada*

**Distribución de congestion_level:**

![Joel: umbrales P33/P66 y distribución de niveles de congestión](img/03_distribucion_congestion.png)
*Figura — Joel: umbrales P33/P66 y distribución de niveles de congestión*

**Verificación de escritura/lectura Parquet (ida y vuelta):**

![Joel: verificación de ida y vuelta sobre congestion_silver](img/04_partition_filters.png)
*Figura — Joel: verificación de ida y vuelta sobre congestion_silver*

**explain(True) con PartitionFilters:**

![Joel: plan de ejecución con PartitionFilters](img/05_tabla_comparativa.png)
*Figura — Joel: plan de ejecución con PartitionFilters (partition pruning)*

**Tabla comparativa LR vs RF y Random Forest ganador guardado:**

![Joel: tabla comparativa LR vs RF y Random Forest ganador guardado](img/06_random_forest_ganador.png)
*Figura — Joel: tabla comparativa LR vs RF y Random Forest ganador guardado*

---

## 7. Trazabilidad con la malla curricular

| Criterio | CE | Evidencia |
| :--- | :--- | :--- |
| Arquitectura Lambda justificada | CE042-N3 | Sección 1 |
| PySpark para extracción, transformación, agregación | CE042-N3 | Sección 2 |
| Parquet particionado y verificado | CE042-N3 | Sección 3 |
| Pipeline batch reproducible | CE042-N3 | Notebooks completos |
| Componente ML con métricas y comparación | CE043-N3 (parcial) | Sección 4 |

---

## 8. Anexos

- Brief Técnico-Analítico: `docs/brief-tecnico-analitico.md`
- Arquitectura Lambda: `docs/arquitectura-lambda.md`
- Notebook Leonardo: `notebooks/s04_regresion_retrasos_leonardo.ipynb`
- Notebook Joel: `notebooks/s04_clasificacion_congestion_joel.ipynb`
- Repositorio: https://github.com/LeoNarD1812/bigdata-ps-grupoXX-aire
