# Evidencia

Esta carpeta contiene la evidencia real de las ejecuciones. No hay resultados inventados.

## 4.2 — ejecución en Colab

La corrida se realizó el **27 de octubre de 2026** en Google Colab, con Kafka 4.0.0 y el commit `b221522` del repositorio. Los archivos están en [`4.2_producer/`](4.2_producer/):

- `source_manifest.json`
- `producer_summary.json`
- `producer.log`
- `topic_description.txt`
- `sample_messages.json`
- `partition_counts.json`
- `run_environment.json`
- `checks.json`

Resultado: **10.000 eventos intentados, 10.000 entregados, 0 fallidos, 0 pendientes**. Los 17 checks están en `true`.

No se deben agregar aquí logs completos de Kafka, checkpoints de Spark, Parquet de resultados, archivos temporales ni credenciales.

## 4.3 — ingesta con Spark

La corrida se realizó en Colab con Spark 4.0.1 y Kafka 4.0.0. Los archivos están en [`4.3_ingest/`](4.3_ingest/):

- `first_run.json`: 10.000 eventos.
- `restart_run.json`: 5 eventos nuevos.
- `offsets_comparison.json`: sin reprocesamiento.
- `schema.json`, `batch_metrics.jsonl` y muestras.
- `checks.json`: 15/15 en `true`.

El reinicio comenzó exactamente en los offsets donde terminó la primera corrida.

## 4.4 — consultas de streaming

La corrida se realizó en Colab con Spark 4.0.1 y Kafka 4.0.0. Los archivos están en [`4.4_queries/`](4.4_queries/):

- `checks.json`: 19/19 en `true`.
- `evidence_summary.json`: entorno, offsets, consultas y watermark.
- `queries_summary.json`: configuración y registros de las consultas.
- `state_top10.json` y `hub_top3.json`: últimas fotos de los rankings.
- `window_metrics.jsonl`: 1.553 ventanas cerradas.
- `watermark_test/`: eventos, resumen y ventanas emitidas.

Resultado: 10 filas de estados, 9 filas de hubs, 1.553 ventanas cerradas, `LATE_OK` presente y `LATE_DROP` ausente. No se incluyen Parquet de resultados, checkpoints ni logs completos de Kafka.

## 4.5 — capacidad y cuello de botella

La corrida se realizó en Colab con Spark 4.0.1 y Kafka 4.0.0. Los archivos están en [`4.5_capacity/`](4.5_capacity/):

- `checks.json`: 9/9 en `true`.
- `capacity_summary.json`: resumen de los tres escenarios.
- `capacity_metrics.csv`: muestras de tasas, lag, batches y CPU/RSS.
- `capacity_plot.png`: gráfico de capacidad.
- `scenario_50.json`, `scenario_250.json` y `scenario_500.json`: detalle por escenario.

Resultado: 50/s entregó 2.867 eventos (47,77/s); 250/s entregó 12.213 (203,54/s); 500/s entregó 21.336 (355,57/s). Todos los eventos entregados se procesaron y el lag final fue 0. El primer límite observado está en el Producer/entorno local. En 500 el batch máximo superó el trigger de 5 s, pero no se puede concluir que Spark soporte 500 reales porque el Producer no llegó a esa tasa.

No se incluyen Parquet de ventanas, checkpoints ni logs completos.

## 4.6 — recuperación y at-least-once

La corrida se realizó en Colab con Spark 4.0.1 y Kafka 4.0.0. Los archivos están en [`4.6_recovery/`](4.6_recovery/):

- `checks.json`: 17/17 en `true`.
- `recovery_summary.json`: configuración, offsets, estados y productores.
- `duplicate_summary.json`: prueba de duplicado controlado.
- `duplicate/fallo_inducido.flag`: marca del fallo inducido.

Resultado: primera corrida 200, reinicio sin backlog 0, backlog 300 con estado 500, contraste `latest` 50 y duplicado 100 → 200 filas con 100 `event_id` únicos. El checkpoint principal reanudó sin reprocesar; la consulta nueva `latest` ignoró el backlog. No se incluyen snapshots Parquet, checkpoints ni el sink completo.
