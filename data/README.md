# Datos de la Tarea 2

Este directorio contiene únicamente la porción del proyecto de Tarea 1 necesaria para el replay de 4.2.

## Archivos

| Archivo | Filas | Columnas | Tamaño |
|---|---:|---:|---:|
| `fact_flights_sep_sorted.parquet` | 582.622 | 12 | 5.826.932 bytes |
| `dim_airports.parquet` | 348 | 3 | 10.097 bytes |

El detalle de hashes, nulos y procedencia está en [`source_manifest.json`](source_manifest.json).

## Origen

- Dataset original: *US Flight Data 2024*, Kaggle / BTS.
- Artefacto de origen: `data_clean/fact_flights/month=9` y `data_clean/dim_airports` de la Tarea 1.
- El Parquet de vuelos conserva solo las columnas que usa el Producer:

```text
fl_date, op_unique_carrier, op_carrier_fl_num, origin, dest,
crs_dep_time, dep_time, dep_delay, arr_delay, cancelled,
diverted, distance
```

- Orden: `(fl_date, crs_dep_time)` ascendente.
- `event_time` se calcula en el Producer como `fl_date + crs_dep_time` (hora programada local).
- No se imputan retrasos ni se eliminan vuelos cancelados.

## Qué no está incluido

- El CSV crudo de 1,3 GB.
- El `fact_flights` completo de 7.079.081 filas.
- Los datos de Kafka, checkpoints o resultados de Spark.

El repositorio no necesita esos archivos para ejecutar 4.2. Para el año completo se debe volver al proyecto de Tarea 1 o a la fuente pública de Kaggle.
