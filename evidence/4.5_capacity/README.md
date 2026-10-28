# Evidencia de 4.5 — experimento de capacidad

- **Fecha:** 2026-10-27
- **Runtime:** Google Colab, Python 3.13.18
- **Spark:** 4.0.1
- **Kafka:** 4.0.0, un nodo KRaft
- **Commit del repositorio:** `2cbe66317f9096bf9d70201526e2d56a6b816bc2`
- **Consulta medida:** Consulta 3, ventanas de 6 h cada 1 h, watermark 3 h
- **Trigger:** `processingTime=5 seconds`
- **Escenarios:** 50, 250 y 500 eventos/s; 60 s de producción y 30 s de cooldown
- **Checkpoints:** uno por escenario, `startingOffsets=latest`

## Resultados

| Tasa objetivo | Entregados | Tasa real | Procesados | Lag final | Lag máximo | Batch máximo | Sink total | Filas de ventana |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 | 2.867 | 47,77/s | 2.867 | 0 | 378 | 2.784 ms | 0,000018 s | 0 |
| 250 | 12.213 | 203,54/s | 12.213 | 0 | 1.935 | 4.518 ms | 15,825 s | 2.690 |
| 500 | 21.336 | 355,57/s | 21.336 | 0 | 3.361 | 5.239 ms | 23,472 s | 4.868 |

Los tres escenarios terminaron con `status=ok`, 0 fallos de entrega y 0 atraso final después del cooldown.

## Interpretación

### El primer límite observado es el Producer

La tasa real de entrega se aleja del objetivo al aumentarlo:

```text
50  → 47,77/s   (95,5 % del objetivo)
250 → 203,54/s  (81,4 % del objetivo)
500 → 355,57/s  (71,1 % del objetivo)
```

En 4.2, con objetivo 500, el Producer había logrado 381 eventos/s. Los resultados son coherentes: el Producer no sostiene 500 eventos/s en este entorno. Como todos los eventos entregados terminaron procesados y el lag final es 0, no hay evidencia de una pérdida o de un atraso permanente en Kafka/Spark.

El Producer, el broker y Spark comparten el mismo host de Colab. La limitación observada puede ser del Producer, de la CPU compartida o del broker; la evidencia no permite separarlos con precisión. Lo que sí se puede afirmar es que **la tasa de llegada real quedó limitada antes de que Spark mostrara un atraso permanente**.

### Spark muestra presión en el escenario de 500

En el escenario de 500:

- el batch máximo fue **5.239 ms**, superior al trigger de 5 s;
- el lag máximo observado fue **3.361 eventos**;
- después del cooldown, el lag volvió a **0**.

Eso indica que Spark estuvo cerca de su límite con la tasa que realmente recibió —un máximo de `inputRowsPerSecond` de 421,4—, pero no permite concluir que pueda sostener 500 eventos/s, porque el Producer nunca entregó 500. La ventana y el sink Parquet son candidatos claros a ese coste: el sink acumuló 23,47 s de escritura en el escenario de 500.

### Kafka y particiones

El topic tiene 3 particiones y un solo broker. No se observó un error de Kafka ni un atraso final. La evidencia no permite atribuir el límite al broker o a las particiones por separado; para eso harían falta métricas del broker y pruebas con más particiones o brokers. La configuración de 3 particiones sigue siendo una restricción de paralelismo del laboratorio.

### El escenario de 50 no produjo ventanas

En el escenario de 50 se entregaron 2.867 eventos en 60 s. El rango de `event_time` no avanzó lo suficiente para superar el watermark de 3 horas, así que no se cerró ninguna ventana de 6 horas. **No es un fallo**: la consulta de ventanas solo emite ventanas cerradas. La medición de throughput sigue siendo válida. En 250 y 500 sí se emitieron ventanas y se ejerció el sink.

## Conclusión

Para el entorno de laboratorio:

1. **Cuello de botella principal observado:** Producer/entorno local, que no alcanza 250 ni 500 eventos/s.
2. **Spark:** procesa todo lo entregado y drena el atraso, pero en 500 el batch máximo supera el trigger y el margen es estrecho; no se puede afirmar que soporte 500 reales.
3. **Kafka:** sin errores ni atraso final; no hay evidencia suficiente para culpar al broker o a las 3 particiones.
4. **Sink:** la escritura Parquet es una parte relevante del coste en 250/500; se midió su tiempo dentro de `foreachBatch`.

## Archivos

- `checks.json` — 9/9 checks.
- `capacity_summary.json` — resumen de escenarios y checks.
- `capacity_metrics.csv` — muestras de tasas, lag, batches y CPU/RSS.
- `capacity_plot.png` — gráfico de capacidad.
- `scenario_50.json`, `scenario_250.json`, `scenario_500.json` — detalle de cada corrida.
- No se incluyen los Parquet de ventanas, checkpoints ni logs completos.
