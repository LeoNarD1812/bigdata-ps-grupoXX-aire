# Arquitectura Big Data del Proyecto Sello

## Decisión: Lambda

El caso de negocio (predicción de retrasos y congestión aérea) necesita **histórico + tiempo real**, por lo tanto corresponde **Lambda**.

## Diagrama

```mermaid
flowchart LR
    subgraph Fuentes["Fuentes de datos"]
        A1[Kaggle: Flight Delay 2024\n7M filas históricas]
        A2[OpenSky API\nposiciones en vivo - U2]
    end

    subgraph Batch["Batch Layer - U1"]
        B1[PySpark ETL\nlimpieza + transformaciones]
        B2[Parquet particionado\nsalida Gold]
        B3[Modelos MLlib\nRegresión + Clasificación]
    end

    subgraph Speed["Speed Layer - U2"]
        C1[Kafka topic\nopensky-positions]
        C2[Spark Structured Streaming\nventanas de tiempo]
    end

    subgraph Serving["Serving Layer"]
        D1[Notebooks Jupyter\nResultados U1]
        D2[Grafana\nTablero en vivo - U2]
    end

    A1 --> B1 --> B2 --> B3 --> D1
    A2 --> C1 --> C2 --> D2
    B3 -.modelo entrenado.-> C2