# Evidencia de 4.2 — Colab

- **Fecha:** 2026-10-27
- **Runtime:** Google Colab, Python 3.13.18
- **Kafka:** 4.0.0, un nodo KRaft
- **Commit del repo:** `b2215226014617d0cec32cc49824f7ae8382f411`
- **Run ID:** `tarea2-42-20261027T190232Z-b6aff9`

## Resultado

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

La tasa real es menor que la objetivo porque el Producer aplica un límite estricto y no genera ráfagas para recuperar tiempo perdido.

## Topic

```text
flights.events
PartitionCount: 3
ReplicationFactor: 1
retention.ms=604800000
cleanup.policy=delete
message.timestamp.type=LogAppendTime
```

Los mensajes de muestra tienen `timestamp` tipo `2` (`LogAppendTime`) y conservan `event_time` de septiembre de 2024 en el payload.

## Archivos

Los nombres coinciden con los generados por el notebook. `checks.json` contiene los 17 criterios de aceptación y todos están en `true`.
