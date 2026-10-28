# Evidencia de 4.6 — recuperación y at-least-once

- **Fecha:** 2026-10-27
- **Runtime:** Google Colab, Python 3.13.18
- **Spark:** 4.0.1
- **Kafka:** 4.0.0, un nodo KRaft
- **Topic:** `flights.events`
- **Trigger:** `processingTime=5s`
- **Checkpoint principal:** `/content/tarea2_bigdata/checkpoints/4.6_recovery/main`
- **Checkpoint de contraste:** `/content/tarea2_bigdata/checkpoints/4.6_recovery/contrast`
- **Checkpoint de duplicado:** `/content/tarea2_bigdata/checkpoints/4.6_recovery/duplicate`

## Resultados

| Caso | Resultado |
|---|---|
| Primera corrida | 200 eventos; estado 200 |
| Reinicio sin backlog | 0 eventos; estado 200 |
| Backlog durante la pausa | 300 eventos |
| Reinicio con backlog | 300 eventos; estado 500; sin reprocesar |
| Contraste con consulta nueva `latest` | 0 del backlog anterior; 50 nuevos; estado 50 |
| Prueba de duplicado | 100 filas; reinicio; 200 filas, 100 `event_id` únicos y 100 duplicados |
| Checks | 17/17 en `true` |

La consulta principal usó `outputMode("complete")` y una agregación por `origin_state`. El estado acumulado pasó de 200 a 500 después del backlog.

## Offsets

```text
Fin primera corrida: {"0": 16560, "1": 16228, "2": 16135}
Inicio reinicio:      {"0": 16560, "1": 16228, "2": 16135}
Fin backlog:         {"0": 16666, "1": 16323, "2": 16234}
Fin contraste:       {"0": 16696, "1": 16358, "2": 16269}
```

El reinicio con backlog empezó exactamente en el offset final de la primera corrida. No se reprocesaron los 200 eventos anteriores.

## Duplicado controlado

- Se escribieron 100 filas en un sink append-only.
- Se lanzó una excepción inducida después de escribir el sink y antes de confirmar el checkpoint.
- Kafka conservó los 100 eventos.
- Al reiniciar con el mismo checkpoint, Spark reprocesó el batch:
  - 200 filas finales;
  - 100 `event_id` únicos;
  - 100 filas duplicadas.

Esto demuestra el comportamiento **at-least-once**. No se duplicó el mensaje en Kafka; se repitió la salida de Spark porque el checkpoint no había confirmado el batch.

## Archivos

- `checks.json`: 17/17 checks.
- `recovery_summary.json`: configuración, carreras, offsets y estados.
- `duplicate_summary.json`: conteos y muestra de `event_id` duplicados.
- `duplicate/fallo_inducido.flag`: marca del fallo controlado.
- No se incluyen checkpoints, snapshots Parquet ni el sink completo.

## Interpretación

- El mismo checkpoint reanuda desde la posición guardada.
- Kafka funciona como buffer y conserva el backlog durante la pausa.
- Un checkpoint nuevo con `startingOffsets=latest` no hereda ni el backlog ni el estado.
- El flujo es at-least-once, no exactly-once end-to-end.
