# Evidencia de 4.3 — ingesta con Spark Structured Streaming

- **Fecha:** 2026-10-27
- **Runtime:** Google Colab, Python 3.13.18
- **Spark:** 4.0.1
- **Java:** 21.0.12.1
- **Kafka:** 4.0.0, un nodo KRaft
- **Commit del repo:** `a89ba5df6cec9d0264aaa0cf6afb354ce92d487f`
- **Trigger:** `availableNow`
- **Checkpoint:** `/content/tarea2_bigdata/checkpoints/4.3_ingest`

## Resultado

| Métrica | Valor |
|---|---:|
| Primera corrida | 10.000 eventos |
| Eventos válidos | 10.000 |
| Eventos inválidos | 0 |
| Eventos nuevos enviados | 5 |
| Reinicio con el mismo checkpoint | 5 eventos |
| Total procesado | 10.005 |
| Reprocesamiento de los 10.000 | No |
| Checks | 15/15 en `true` |

## Offsets

```text
Pre-topic:       {"0": 3321, "1": 3406, "2": 3273}
Fin primera:     {"0": 3321, "1": 3406, "2": 3273}
Inicio reinicio: {"0": 3321, "1": 3406, "2": 3273}
Fin reinicio:    {"0": 3323, "1": 3407, "2": 3275}
Kafka final:     {"0": 3323, "1": 3407, "2": 3275}
```

El reinicio comenzó exactamente en los offsets donde terminó la primera corrida. Los 10.000 eventos anteriores no se reprocesaron y los 5 nuevos sí se procesaron.

## Interpretación

- `event_time` se interpretó como timestamp a partir del payload.
- `kafka_timestamp` es `LogAppendTime`, no la hora histórica del vuelo.
- La validación separó válidos e inválidos; en esta corrida no hubo inválidos.
- El checkpoint guardó la posición de Spark; no se hicieron commits manuales de consumer group.
- El flujo es at-least-once: un fallo entre procesamiento y commit del checkpoint podría repetir un batch.

## Archivos

- `first_run.json` — primera corrida.
- `restart_run.json` — reinicio con el mismo checkpoint.
- `offsets_comparison.json` — comparación de offsets.
- `batch_metrics.jsonl` — métricas por batch.
- `schema.json` — esquema del payload y del stream.
- `sample_valid_events.json` — muestra de válidos.
- `sample_invalid_events.json` — muestra de inválidos; vacía en esta corrida.
- `restart_test_events.json` — los 5 eventos nuevos.
- `spark_environment.json` — versiones y configuración.
- `checks.json` — 15/15 en `true`.
- `source_manifest.json` — procedencia de los datos.
