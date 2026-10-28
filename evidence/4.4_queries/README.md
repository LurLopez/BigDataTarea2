# Evidencia de 4.4 — consultas de streaming y watermark

- **Fecha:** 2026-10-27
- **Runtime:** Google Colab, Python 3.13.18
- **Spark:** 4.0.1
- **Java:** 21.0.12.1
- **Kafka:** 4.0.0, un nodo KRaft
- **Commit del repositorio:** `54d322cb8dc1aecb63644b8467d4467e20573e4f`
- **Topic:** `flights.events`
- **Trigger:** `processingTime=5 seconds`
- **Event-time:** `event_time` del payload, calculado desde `crs_dep_time`

## Resultado

| Consulta | Resultado |
|---|---|
| 1 — estados | 10 filas; 10.005 vuelos; mínimo 100 por estado |
| 2 — hubs | 9 filas; 3 por cada hub (ATL, DFW y ORD); mínimo 20 por combinación |
| 3 — ventanas | 1.553 ventanas cerradas; 289 aeropuertos; rango de ventanas 2024-08-31 19:00 → 2024-09-01 11:00 |
| Watermark | 4 eventos entregados; 6 filas de salida; `LATE_OK` presente y `LATE_DROP` ausente |
| Checks | 19/19 en `true` |

La suma de `flights` de las ventanas es **18.791**, mayor que los 10.005 eventos de entrada, porque las ventanas de 6 horas se desplazan cada hora y un mismo vuelo puede pertenecer a varias ventanas.

## Offsets de Kafka

Antes de la prueba de watermark, las consultas procesaron los 10.005 eventos reales. Después se publicaron cuatro eventos sintéticos de prueba:

```text
Offsets finales: {"0": 3324, "1": 3409, "2": 3276}
Total: 10.009
```

Los cuatro eventos sintéticos no forman parte de las consultas principales: están marcados con `test_injected_late=true` y las consultas filtran esa bandera.

## Top 10 de estados

| Posición | Estado | Vuelos | Retraso medio | Desviación | Máximo |
|---:|---|---:|---:|---:|---:|
| 1 | South Carolina | 104 | 22,846 | 181,660 | 1.802 |
| 2 | Virginia | 340 | 8,029 | 51,057 | 563 |
| 3 | Oregon | 132 | 7,091 | 68,805 | 743 |
| 4 | Tennessee | 221 | 6,064 | 61,528 | 755 |
| 5 | Maryland | 124 | 5,669 | 15,085 | 97 |
| 6 | Florida | 753 | 5,416 | 38,945 | 637 |
| 7 | Texas | 962 | 5,200 | 53,248 | 1.263 |
| 8 | New York | 508 | 5,059 | 42,782 | 510 |
| 9 | Washington | 323 | 4,619 | 36,630 | 440 |
| 10 | Kentucky | 121 | 4,339 | 45,412 | 443 |

South Carolina tiene el promedio más alto, pero también una desviación muy grande y un máximo de 1.802 minutos; la media está influida por valores extremos. No debe interpretarse como un ranking estable de puntualidad.

## Top 3 por hub

| Hub | Posición | Aerolínea | Vuelos | Retraso medio de llegada |
|---|---:|---|---:|---:|
| ATL | 1 | 9E | 23 | -7,696 |
| ATL | 2 | WN | 43 | -6,837 |
| ATL | 3 | DL | 248 | -5,548 |
| DFW | 1 | OO | 30 | -22,367 |
| DFW | 2 | MQ | 87 | -4,437 |
| DFW | 3 | AA | 189 | 1,339 |
| ORD | 1 | AA | 68 | -5,303 |
| ORD | 2 | MQ | 33 | -5,030 |
| ORD | 3 | UA | 152 | -4,768 |

Un promedio negativo significa que la aerolínea llegó, en promedio, antes de lo previsto. El ranking es provisional y corresponde a los 10.005 eventos procesados, no al año completo.

## Prueba de watermark

Los eventos de prueba fueron:

| Etiqueta | `event_time` | Resultado |
|---|---|---|
| `CLOCK` | 2030-01-01 04:00 | Fija el máximo inicial |
| `LATE_OK` | 2030-01-01 02:00 | Aceptado; aparece en la agregación |
| `CLOCK` | 2030-01-01 09:00 | Avanza el watermark a 06:00 |
| `LATE_DROP` | 2030-01-01 03:30 | Descartado por la ventana |

`LATE_DROP` se entregó correctamente a Kafka, pero la consulta de ventana lo descartó porque su `event_time` quedó por detrás del watermark. No debe interpretarse como un fallo del Producer.

## Archivos

- `checks.json` — 19/19 checks.
- `evidence_summary.json` — resumen de entorno, offsets, consultas y prueba.
- `queries_summary.json` — configuración y registros de las consultas.
- `state_top10.json` — último top 10.
- `state_top10_history.jsonl` — histórico de snapshots.
- `hub_top3.json` — último top 3 por hub.
- `hub_top3_history.jsonl` — histórico de snapshots.
- `window_metrics.jsonl` — 1.553 ventanas cerradas.
- `window_sample_1.json` — muestra de las primeras 100 ventanas.
- `watermark_test/test_events.json` — eventos sintéticos.
- `watermark_test/test_summary.json` — resumen de la prueba.
- `watermark_test/windows.jsonl` — filas emitidas por la consulta de prueba.

No se incluyen en el repositorio los Parquet de resultados, los checkpoints de Spark ni logs completos de Kafka.
