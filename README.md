# Tarea 2 — Kafka y Spark Structured Streaming

Continuidad de la **Tarea 1: US Flight Data 2024** (7.079.081 vuelos, 348 aeropuertos), ahora con un flujo de eventos:

```text
Parquet T1 → Producer → Kafka `flights.events` → Spark Structured Streaming → resultados/métricas
```

La entrega se ejecuta en **Google Colab**. El repositorio es público y contiene los datos necesarios para reproducir 4.2–4.6; no contiene el CSV crudo ni el Parquet completo de Tarea 1.

## Estado

- Secciones 1, 2, 3 y 4.1 documentadas en el notebook.
- Sección 4.2 implementada y **ejecutada en Colab**: fuente de septiembre, Producer, Kafka local, topic, 10.000 eventos y verificación. Evidencia en [`evidence/4.2_producer/`](evidence/4.2_producer/).
- Sección 4.3 implementada y **ejecutada en Colab**: ingesta Spark, esquema explícito, validación, checkpoint y reinicio con 5 eventos. Evidencia en [`evidence/4.3_ingest/`](evidence/4.3_ingest/).
- El notebook incluye las **salidas visibles** de 4.2–4.6 y los mensajes `4.2 OK` a `4.6 OK`.
- Sección 4.4 **implementada y ejecutada en Colab**: tres consultas de streaming, umbrales adaptados al replay, ventanas de 6 h con watermark de 3 h y prueba controlada de eventos tardíos. Evidencia en [`evidence/4.4_queries/`](evidence/4.4_queries/).
- No se incluyen métricas inventadas.
- Sección 4.5 **implementada y ejecutada en Colab**: experimento con 50, 250 y 500 eventos/s, consulta de ventanas, checkpoints por escenario, métricas y gráfico. Evidencia en [`evidence/4.5_capacity/`](evidence/4.5_capacity/).
- Sección 4.6 **implementada y ejecutada en Colab**: reinicio con y sin backlog, contraste `latest` y prueba de duplicado controlado. Evidencia en [`evidence/4.6_recovery/`](evidence/4.6_recovery/).
- Sección 4.7 **implementada y documentada**: comparación batch vs. streaming con los resultados de 4.2–4.6.
- La sección 4.8 está documentada en el notebook: conclusiones, bibliografía y declaración de uso de IA.
- Queda pendiente la defensa del proyecto.

## Resultados de 4.2

| Métrica | Valor |
|---|---:|
| Eventos intentados | 10.000 |
| Eventos entregados | 10.000 |
| Fallidos | 0 |
| Pendientes tras flush | 0 |
| Tiempo del Producer | 26,218 s |
| Tasa real | 381,421 eventos/s |
| Tasa objetivo | 500 eventos/s |
| Particiones | 3 |
| Mensajes por partición | 3321 / 3406 / 3273 |
| Checks verdaderos | 17/17 |

La tasa real es menor que la objetivo porque la tasa es un **límite estricto**: el Producer no genera ráfagas para recuperar tiempo perdido. El run ID fue `tarea2-42-20261027T190232Z-b6aff9`.

## Resultados de 4.3

| Métrica | Valor |
|---|---:|
| Primera corrida | 10.000 eventos |
| Eventos nuevos | 5 |
| Total procesado | 10.005 |
| Válidos | 10.005 |
| Inválidos | 0 |
| Reprocesamiento | No |
| Checks | 15/15 en `true` |

Los offsets pasaron de `3321 / 3406 / 3273` a `3323 / 3407 / 3275`. El reinicio comenzó exactamente en el offset final de la primera corrida y procesó solo los 5 eventos nuevos.

## Resultados de 4.4

| Consulta | Resultado |
|---|---|
| 1 — estados | 10 filas, 10.005 vuelos, mínimo 100 por estado |
| 2 — hubs | 9 filas, 3 por cada hub |
| 3 — ventanas | 1.553 ventanas cerradas, 289 aeropuertos |
| Watermark | 4 eventos entregados; `LATE_OK` presente y `LATE_DROP` ausente |
| Checks | 19/19 en `true` |

La suma de `flights` de las ventanas es 18.791, mayor que los 10.005 eventos, porque las ventanas de 6 horas se solapan cada hora. El rango de ventanas cerradas es 2024-08-31 19:00 → 2024-09-01 11:00.

El top 10 de estados quedó liderado por South Carolina (104 vuelos, retraso medio 22,846, máximo 1.802), seguido de Virginia y Oregon. En los hubs, los mejores promedios de llegada fueron 9E/WN/DL en ATL, OO/MQ/AA en DFW y AA/MQ/UA en ORD. Los promedios negativos indican llegadas tempranas.

La prueba de watermark confirmó el comportamiento esperado: `LATE_OK` apareció en la agregación y `LATE_DROP` no. El evento `LATE_DROP` sí se entregó a Kafka; la consulta lo descartó porque quedó por detrás del watermark. Los offsets finales fueron `3324 / 3409 / 3276` (10.009 eventos, incluidos los 4 sintéticos).

## Consultas de 4.4

La sección 4.4 implementa tres consultas sobre `valid_events` y un checkpoint independiente por consulta. Las dos primeras son continuidad de Tarea 1; la tercera usa tiempo de evento y watermark.

| Consulta | Operación | Salida | Checkpoint |
|---|---|---|---|
| 1 — `state` | `count`, `avg`, `stddev` y `max` de `dep_delay` por estado; top 10 con al menos 100 vuelos | Parquet y JSON de la última foto | `4.4_queries/state` |
| 2 — `hubs` | `count` y `avg(arr_delay)` por hub/aerolínea en ATL, DFW y ORD; mínimo 20 vuelos; top 3 por hub | Parquet y JSON de la última foto | `4.4_queries/hubs` |
| 3 — `window` | Ventana deslizante de 6 h cada 1 h por aeropuerto; vuelos, retraso medio y porcentaje cancelado; watermark de 3 h | Parquet de ventanas cerradas | `4.4_queries/window` |

Los umbrales de 100 y 20 son una adaptación al replay de 10.005 eventos. Los umbrales de Tarea 1 (10.000 vuelos por estado y 500 por aerolínea/hub) no son alcanzables con ese volumen y no se presentan como equivalentes anuales.

La prueba de 4.4 publica eventos sintéticos de 2030 marcados con `test_injected_late=true` en el mismo topic, con checkpoint nuevo y `startingOffsets=latest`: `LATE_OK` llega dentro del watermark y debe aparecer; `LATE_DROP` llega por detrás y debe quedar fuera de la agregación. Las consultas principales filtran esos eventos y se detienen antes de la prueba.


## Experimento de capacidad de 4.5

La sección 4.5 mide una sola consulta, la de ventanas, con tres tasas de llegada:

| Escenario | Tasa objetivo | Duración | Cooldown |
|---|---:|---:|---:|
| Bajo | 50 eventos/s | 60 s | 30 s |
| Medio | 250 eventos/s | 60 s | 30 s |
| Alto | 500 eventos/s | 60 s | 30 s |

Cada escenario usa un checkpoint nuevo, un stream Kafka con `startingOffsets=latest` y filas nuevas del Parquet a partir de la fila 10.005. Se registran tasa real del Producer, `inputRowsPerSecond`, `processedRowsPerSecond`, duración de batches, atraso, CPU/RSS y tiempo de escritura del sink.

La salida se genera en:

```text
/content/tarea2_bigdata/outputs/4.5_capacity/
├── scenario_50.json
├── scenario_250.json
├── scenario_500.json
├── capacity_metrics.csv
├── capacity_plot.png
├── capacity_summary.json
└── checks.json
```

### Resultados reales

| Objetivo | Entregados | Tasa real | Procesados | Lag final | Lag máximo | Batch máximo |
|---:|---:|---:|---:|---:|---:|---:|
| 50/s | 2.867 | 47,77/s | 2.867 | 0 | 378 | 2.784 ms |
| 250/s | 12.213 | 203,54/s | 12.213 | 0 | 1.935 | 4.518 ms |
| 500/s | 21.336 | 355,57/s | 21.336 | 0 | 3.361 | 5.239 ms |

Los 9 checks quedaron en `true`. El Producer no alcanza las tasas de 250 y 500: entrega el 81,4 % y el 71,1 % del objetivo, respectivamente. Como todos los eventos entregados terminaron procesados y el lag final es 0, el primer límite observado está en el Producer/entorno local, no en un atraso permanente de Kafka o Spark.

En el escenario de 500 Spark mostró presión: el batch máximo fue 5.239 ms, superior al trigger de 5 s, y el lag máximo llegó a 3.361 eventos, aunque se drenó durante el cooldown. No se puede concluir que Spark soporte 500 eventos/s porque el Producer nunca entregó esa tasa. El sink Parquet acumuló 23,47 s de escritura en ese escenario.

El escenario de 50 no cerró ventanas porque su `event_time` no superó el watermark de 3 horas; no es un fallo, la consulta solo emite ventanas cerradas. Los escenarios de 250 y 500 sí emitieron ventanas y ejercieron el sink.

La evidencia completa está en [`evidence/4.5_capacity/`](evidence/4.5_capacity/), incluido el gráfico `capacity_plot.png`.

## Recuperación y comparación final

La sección 4.6 implementa:

- una consulta de estado con el mismo checkpoint;
- un reinicio sin backlog;
- un reinicio con backlog;
- un contraste con checkpoint nuevo y `startingOffsets=latest`;
- una prueba separada de duplicado inducido entre sink y checkpoint.

La sección 4.7 compara Tarea 1 y Tarea 2 en disponibilidad del dataset, resultados, particionado, procesamiento, ventanas, persistencia, recuperación y costes. La comparación es arquitectónica y semántica, no un benchmark de rendimiento entre volúmenes distintos. La sección 4.8 cierra conclusiones, bibliografía y declaración de uso de IA.

La ejecución de referencia de 4.6 terminó con `4.6 OK` y 17/17 checks. La evidencia está en [`evidence/4.6_recovery/`](evidence/4.6_recovery/).

## Resultados de 4.6

| Caso | Resultado |
|---|---|
| Primera corrida | 200 eventos; estado 200 |
| Reinicio sin backlog | 0 eventos; estado 200 |
| Backlog durante la pausa | 300 eventos |
| Reinicio con backlog | 300 eventos; estado 500; sin reprocesar |
| Contraste `latest` | 0 del backlog; 50 nuevos; estado 50 |
| Duplicado inducido | 100 → 200 filas; 100 `event_id` únicos |
| Checks | 17/17 en `true` |

Los offsets del reinicio con backlog empezaron en `16560 / 16228 / 16135`, exactamente donde terminó la primera corrida. La prueba de duplicado demuestra el comportamiento at-least-once: Kafka conserva 100 mensajes y el sink append-only termina con 200 filas.






## Cómo ejecutar

1. Abrir el notebook en Colab:

   ```text
   https://colab.research.google.com/github/LurLopez/BigDataTarea2/blob/main/notebooks/Tarea2_Kafka_Spark_Streaming.ipynb
   ```

2. Ejecutar las celdas en orden.
3. La celda de preparación clona este repositorio en `/content/BigDataTarea2`, instala `confluent-kafka` y `pyarrow`, y valida el hash, el conteo y el orden del Parquet.
4. La celda de Kafka instala Java 17 si hace falta, descarga Kafka 4.0.0, arranca un nodo KRaft y crea `flights.events`.
5. La celda del Producer envía **10.000 eventos** a **500 eventos/s** (tasa estricta).
6. La celda de verificación consulta el topic, cuenta offsets por partición y guarda la evidencia en:

   ```text
   /content/tarea2_bigdata/outputs/4.2_producer/
   ```

7. La sección 4.3 instala PySpark 4.0.1 y el conector Kafka, lee los 10.000 eventos, valida el payload, prueba el checkpoint con 5 eventos nuevos y deja Spark activo para 4.4. La ejecución de referencia terminó con `4.3 OK` y 15/15 checks.

   La evidencia de 4.3 se genera en `/content/tarea2_bigdata/outputs/4.3_ingest/`.

   **Importante:** 4.3 asume que el topic tiene exactamente 10.000 eventos antes de empezar. Si se vuelve a ejecutar 4.3 después de enviar los 5 eventos de prueba, hay que reiniciar el topic y reproducir 4.2 para dejarlo limpio.

8. La sección 4.4 ejecuta las tres consultas con `processingTime=5 seconds`, guarda resultados en `/content/tarea2_bigdata/outputs/4.4_queries/` y usa checkpoints separados. Después ejecuta la prueba de eventos tardíos; la ejecución de referencia terminó con `4.4 OK` y 19/19 checks.

9. La sección 4.5 ejecuta los escenarios de 50, 250 y 500 eventos/s con la consulta de ventanas, checkpoints independientes y `startingOffsets=latest`. La ejecución de referencia terminó con `4.5 OK`, 9/9 checks y el gráfico de capacidad.

10. La sección 4.6 reinicia una consulta de estado con el mismo checkpoint y contrasta con una consulta nueva `latest`. Al final ejecuta una prueba de duplicado inducido; la ejecución de referencia terminó con `4.6 OK` y 17/17 checks. La sección 4.7 es la comparación final y no ejecuta un experimento nuevo.

No se necesita acceso a Google Drive ni a una cuenta personal para descargar los datos. Solo se necesita una cuenta de Google para ejecutar Colab.

La **primera ejecución en un runtime nuevo** descarga Kafka 4.0.0 (126 MB) desde `archive.apache.org`; puede tardar alrededor de 10 minutos porque ese mirror es lento y la celda no muestra barra de progreso. En la misma sesión, volver a ejecutar las celdas de Kafka es rápido: se reutiliza `/content/kafka` y el puerto 9092. Si Colab reinicia el runtime, hay que descargar Kafka de nuevo.

La instalación de PySpark y del conector Kafka también puede tardar unos minutos la primera vez. Después queda en la caché de Ivy y en el entorno de Python de la sesión.

## Datos incluidos

| Archivo | Filas | Descripción |
|---|---:|---|
| `data/fact_flights_sep_sorted.parquet` | 582.622 | Septiembre de 2024, 12 columnas, ordenado por `(fl_date, crs_dep_time)`. |
| `data/dim_airports.parquet` | 348 | Código, ciudad y estado del aeropuerto. |
| `data/source_manifest.json` | — | Procedencia, conteos, columnas, hashes SHA-256 y nulos. |

El Parquet fue derivado del `fact_flights` limpio de Tarea 1. No se subió el CSV de 1,3 GB ni el año completo.

## Decisiones principales

- **Un topic:** `flights.events`.
- **Particiones:** 3 en el laboratorio; RF 1.
- **Retención:** 7 días.
- **Timestamp del evento:** `crs_dep_time` programado, tratado como hora local.
- **Timestamp Kafka:** `LogAppendTime`, para que la retención no borre mensajes históricos de 2024.
- **Tasa:** estricta, sin ráfagas de recuperación.
- **Entrega:** `acks=all`, idempotencia y 5 intentos totales por mensaje.
- **Fallo:** si un mensaje no se entrega tras los intentos, se aborta la corrida.
- **Formato:** JSON UTF-8, `schema_version=1`, headers `source`, `schema_version` y `event_type`.
- **Consultas:** tres consultas independientes, con `processingTime=5 seconds` y un checkpoint por consulta.
- **Capacidad:** una consulta de ventanas, tres tasas (50/250/500) y checkpoints separados; el objetivo es encontrar el límite, no maximizar la tasa.
- **Ventana:** 6 h con desplazamiento de 1 h y watermark de 3 h; solo se publican ventanas cerradas.
- **Eventos tardíos:** prueba aislada con checkpoint nuevo y `startingOffsets=latest`; `LATE_OK` se conserva y `LATE_DROP` se descarta.
- **Recuperación:** mismo checkpoint para reanudar; `latest` para una consulta nueva; prueba de duplicado inducido para documentar at-least-once.
- **Sin exactly-once end-to-end:** el flujo es at-least-once; el sink de Spark usará batches idempotentes.

## Estructura

```text
BigDataTarea2/
├── README.md
├── requirements.txt
├── producer.py
├── arquitectura.png
├── arquitectura.svg
├── arquitectura.md
├── data/
│   ├── fact_flights_sep_sorted.parquet
│   ├── dim_airports.parquet
│   ├── source_manifest.json
│   └── README.md
├── notebooks/
│   └── Tarea2_Kafka_Spark_Streaming.ipynb
├── tests/
│   └── test_producer.py
└── evidence/
    ├── README.md
    ├── 4.2_producer/
    │   ├── checks.json
    │   ├── producer_summary.json
    │   ├── producer.log
    │   ├── topic_description.txt
    │   ├── sample_messages.json
    │   ├── partition_counts.json
    │   ├── run_environment.json
    │   └── source_manifest.json
    ├── 4.3_ingest/
        │   ├── checks.json
        │   ├── first_run.json
        │   ├── restart_run.json
        │   ├── offsets_comparison.json
        │   ├── schema.json
        │   ├── batch_metrics.jsonl
        │   ├── sample_valid_events.json
        │   ├── sample_invalid_events.json
        │   ├── restart_test_events.json
        │   ├── spark_environment.json
        │   └── source_manifest.json
    ├── 4.4_queries/
        │   ├── checks.json
        │   ├── evidence_summary.json
        │   ├── queries_summary.json
        │   ├── state_top10.json
        │   ├── state_top10_history.jsonl
        │   ├── hub_top3.json
        │   ├── hub_top3_history.jsonl
        │   ├── window_metrics.jsonl
        │   ├── window_sample_1.json
        │   └── watermark_test/
    ├── 4.5_capacity/
        ├── checks.json
        ├── capacity_summary.json
        ├── capacity_metrics.csv
        ├── capacity_plot.png
        ├── scenario_50.json
        ├── scenario_250.json
        └── scenario_500.json
    └── 4.6_recovery/
        ├── checks.json
        ├── recovery_summary.json
        ├── duplicate_summary.json
        └── duplicate/
```

## Requisitos

El notebook instala lo necesario en Colab. Para ejecución local:

```bash
pip install -r requirements.txt
```

El laboratorio de Colab usa Kafka 4.0.0 con Java 17+ y un único nodo KRaft. No usa Docker.

## Fuente y atribución

- Patil, H. (2024). *US Flight Data 2024* [conjunto de datos]. Kaggle. <https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024>
- Datos originales: Bureau of Transportation Statistics, U.S. Department of Transportation.
- El proyecto de Tarea 1 realizó la limpieza con Dask y generó `fact_flights` y `dim_airports`.
