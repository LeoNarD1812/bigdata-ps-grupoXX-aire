# Brief Técnico-Analítico del Proyecto Sello

Este documento es el hito de S2 del cronograma. El equipo declara por escrito qué sistema Big Data va a construir durante el ciclo.

---

## 1. Datos del equipo

- **Nombre del equipo:** Grupo XX — Aire
- **Sección:** G1
- **Repositorio (URL):** https://github.com/LeoNarD1812/bigdata-ps-grupoXX-aire
- **Topics del repositorio configurados (sí/no):** Sí — campus-juliaca, semestre-2026-2, linea-cdia, tipo-ps, bigdata, seccion-g1, grupo-XX-aire

### Integrantes

| Integrante | Rol o énfasis previsto |
| :--- | :--- |
| Leonardo Jesús Huamán Arhuata | Batch/Spark + ML (Regresión de retrasos) |
| Joel Jhonathan Quispe Mamani | Batch/Spark + ML (Clasificación de congestión) |

*Este rol es un énfasis de coordinación a nivel de equipo, no un reemplazo del trabajo individual: cada integrante igual construye de extremo a extremo sus dos dimensiones (U1 y U2).*

---

## 2. Dominio del proyecto

- **Nombre del proyecto:** Anticipación de retrasos y congestión en la operación aérea

- **Problema o necesidad que resuelve (2-4 líneas):** Las aerolíneas y aeropuertos operan con información limitada sobre retrasos y congestión esperados. Un sistema que prediga ambos fenómenos a partir de datos históricos permite ajustar buffers de tiempo, planificar turnos de personal y optimizar el uso de pistas, reduciendo costos operativos y mejorando la experiencia del pasajero.

- **Dominio de datos:** Aviación comercial. Se registran vuelos realizados con fecha, aerolínea, origen, destino, hora programada, hora real, minutos de retraso, distancia, duración, y descomposición del retraso por causa (aerolínea, clima, NAS, seguridad, aeronave tardía).

- **Pregunta central de negocio que responde el sistema completo:** ¿Cómo anticipar retrasos y congestión en la operación aérea para optimizar la planificación de vuelos comerciales?

- **Usuarios / actores principales:** Analistas de operaciones de aerolíneas, coordinadores de torre de control, gerentes de planificación de vuelos, y pasajeros (como consumidores indirectos de las alertas).

- **Arquitectura Big Data prevista:** **Lambda**. Justificación: el caso necesita tanto análisis histórico (retrasos pasados para entrenar modelos) como análisis en tiempo real (estado actual del espacio aéreo). La fuente batch alimenta la capa histórica; la fuente streaming (OpenSky en vivo) alimentará la capa de velocidad en la Unidad 2.

- **Fuente de datos batch:** Dataset "Flight Delay Dataset 2024" (basado en BTS On-Time Performance). 7,079,081 filas, 35 columnas, ~1.22 GB. Cubre todo el año 2024. Fuente: Kaggle — patrickzel/flight-delay-and-cancellation-dataset-2019-2023.

- **Fuente de eventos en tiempo real (para U2):** OpenSky Network API (posiciones ADS-B en vivo de aeronaves, actualizadas cada 10-30 segundos). No se implementa en U1.

- **¿Continúa un proyecto de un ciclo anterior, o es un dominio nuevo?** Es un dominio nuevo.

---

## 3. Dimensiones de análisis y fuentes previstas

### Tabla de asignación

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch |
| :--- | :--- | :--- | :--- | :--- |
| Leonardo | Predictiva U1 (batch) | ¿Cuántos minutos de retraso se puede esperar en un vuelo según su ruta, aerolínea, hora programada y distancia? | Minutos de retraso proyectados por un modelo de regresión | Histórico de vuelos 2024 (Kaggle) |
| Leonardo | Predictiva U2 (streaming) | ¿Cuál será el retraso esperado de un vuelo en las próximas horas, según su posición y velocidad actual? | Minutos de retraso proyectados en vivo por vuelo activo | Mismo histórico + OpenSky API en vivo |
| Joel | Predictiva U1 (batch) | ¿Qué probabilidad tiene un aeropuerto de estar congestionado según la hora, día de la semana y flujo histórico de vuelos? | Nivel de congestión (baja/media/alta) estimado por un modelo de clasificación | Mismo histórico, agregado por aeropuerto y hora |
| Joel | Predictiva U2 (streaming) | ¿Qué zonas del espacio aéreo tienen mayor probabilidad de congestionarse en los próximos 30 minutos? | Probabilidad de congestión por zona, calculada en vivo | Mismo histórico + OpenSky API en vivo |

---

### Ficha de dimensión: Retraso de vuelo (Leonardo) — U1

- **Tipo:** Predictiva sin tiempo real (U1, batch)

- **Dimensión, redactada como pregunta completa:** ¿Cuántos minutos de retraso se puede esperar en un vuelo según su ruta, aerolínea, hora programada y distancia? Se relaciona con la pregunta central porque ataca directamente la mitad del problema: el retraso individual de cada vuelo.

- **Indicador(es):** Minutos de retraso promedio por ruta; minutos de retraso proyectados por el modelo de regresión. El KPI final es el RMSE del modelo.

- **Decisión o acción que habilita:** Que una aerolínea ajuste buffers de tiempo en rutas problemáticas y notifique proactivamente a los pasajeros.

- **Instrumento/fuente batch:** Dataset "Flight Delay Dataset 2024" (7,079,081 filas, 35 columnas). Esquema clave: `fl_date`, `op_unique_carrier`, `origin`, `dest`, `crs_dep_time`, `dep_delay`, `distance`, `crs_elapsed_time`, `month`, `day_of_week`. Origen: Kaggle.

- **Instrumento/fuente streaming:** No aplica en U1 (se usará OpenSky en U2).

- **Modelo predictivo:** Regresión (LinearRegression + RandomForestRegressor con comparación de 3 configuraciones de regularización). Corre una sola vez sobre el histórico.

- **Salida (U1):** Notebook Jupyter con tablas de comparación de modelos, gráficos de importancia de variables y la predicción incluida. Salida Parquet particionada por mes.

- **¿Con qué otra(s) dimensión(es) del equipo se combina en el tablero final?** Con la dimensión de congestión de Joel, para formar el tablero "Salud de la operación aérea".

- **Lista inicial de requisitos (mínimo 3):**
  1. El sistema debe cargar 7M de filas con esquema explícito usando `StructType`.
  2. El sistema debe limpiar nulos y duplicados antes de entrenar, documentando el criterio.
  3. El sistema debe comparar al menos dos algoritmos de regresión y guardar el ganador.

---

### Ficha de dimensión: Congestión de aeropuerto (Joel) — U1

- **Tipo:** Predictiva sin tiempo real (U1, batch)

- **Dimensión, redactada como pregunta completa:** ¿Qué probabilidad tiene un aeropuerto de estar congestionado según la hora, día de la semana y flujo histórico de vuelos? Se relaciona con la pregunta central porque ataca la otra mitad: la saturación del espacio aéreo y los aeropuertos.

- **Indicador(es):** Nivel de congestión (0=baja, 1=media, 2=alta); probabilidad estimada por el modelo. El KPI final es el AUC del modelo.

- **Decisión o acción que habilita:** Que un aeropuerto planifique turnos de personal y pistas según la congestión esperada.

- **Instrumento/fuente batch:** El mismo dataset de vuelos 2024, agregado por `origin` + `crs_dep_hour` + `fl_date` para derivar `congestion_level`. Origen: Kaggle.

- **Instrumento/fuente streaming:** No aplica en U1 (se usará OpenSky en U2).

- **Modelo predictivo:** Clasificación (LogisticRegression + RandomForestClassifier). Corre una sola vez sobre el histórico.

- **Salida (U1):** Notebook Jupyter con tablas de comparación, matriz de confusión y predicción incluida. Salida Parquet particionada por `airport_code`.

- **¿Con qué otra(s) dimensión(es) del equipo se combina en el tablero final?** Con la dimensión de retrasos de Leonardo, para el mismo tablero final.

- **Lista inicial de requisitos (mínimo 3):**
  1. El sistema debe agregar los 7M de vuelos por aeropuerto y hora.
  2. El sistema debe derivar una variable categórica de congestión en 3 niveles con criterio documentado.
  3. El sistema debe balancear las clases si es necesario antes de clasificar.

---

### Ficha de dimensión: Retraso en vivo (Leonardo) — U2

- **Tipo:** Predictiva con tiempo real (U2, streaming)
- **Dimensión como pregunta:** ¿Cuál será el retraso esperado de un vuelo en las próximas horas, según su posición y velocidad actual?
- **Indicador:** Minutos de retraso proyectados en vivo, por vuelo activo
- **Decisión que habilita:** Alertas tempranas a torres de control sobre vuelos con riesgo de retraso
- **Instrumento streaming:** OpenSky Network API → Kafka → Spark Structured Streaming
- **Modelo:** El regresor de U1 aplicado en vivo (inferencia)
- **Salida:** Panel en Grafana (compartido con Joel) con retraso proyectado por vuelo
- **Requisitos:**
  1. El sistema debe consultar OpenSky cada 30 segundos.
  2. El sistema debe publicar cada posición a un topic de Kafka.
  3. El sistema debe aplicar el modelo entrenado en U1 sobre el flujo.

---

### Ficha de dimensión: Congestión en vivo (Joel) — U2

- **Tipo:** Predictiva con tiempo real (U2, streaming)
- **Dimensión como pregunta:** ¿Qué zonas del espacio aéreo tienen mayor probabilidad de congestionarse en los próximos 30 minutos?
- **Indicador:** Probabilidad de congestión por zona, calculada en vivo
- **Decisión que habilita:** Reruteo preventivo de aeronaves antes de que la zona se sature
- **Instrumento streaming:** Mismo flujo de OpenSky → Kafka → ventanas de 5 min
- **Modelo:** El clasificador de U1 aplicado en vivo
- **Salida:** Panel en Grafana (compartido) con mapa de calor de congestión
- **Requisitos:**
  1. El sistema debe agrupar posiciones por zona geográfica en ventanas de tiempo.
  2. El sistema debe aplicar el modelo de clasificación de U1.
  3. El sistema debe publicar la probabilidad de congestión por zona.

---

### Qué SÍ cubre este proyecto en conjunto

- Un pipeline batch de ETL distribuido con PySpark sobre 7M de registros reales.
- Salidas analíticas en formato Parquet particionado.
- Dos modelos predictivos entrenados y comparados (regresión + clasificación).
- En U2: streaming con Kafka, Spark Structured Streaming y visualización en Grafana.
- Documentación de decisiones técnicas y evidencias reproducibles.

### Qué NO cubre (fuera de alcance, explícito)

- Modelos de deep learning o redes neuronales.
- Integración con sistemas productivos de aerolíneas.
- Predicción de eventos climáticos extremos no relacionados con vuelos.
- Recomendaciones personalizadas a pasajeros.

---

**Pendiente para las siguientes sesiones:** La infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana, costos, prácticas de DataOps) se construye en clase, sesión por sesión.

---

## 4. Aprobación

- **Docente:** _(pendiente de firma)_
- **Fecha:** _(fecha de presentación del Brief en S2)_