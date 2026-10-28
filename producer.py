#!/usr/bin/env python3
"""Reproduce US Flight Data 2024 como eventos Kafka, leyendo en streaming."""

from __future__ import annotations

import argparse
import calendar
import json
import math
import os
import sys
import time
import uuid
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator


PROJECT_DIR = Path(__file__).resolve().parent
CSV_COLUMNS = [
    "fl_date",
    "op_unique_carrier",
    "op_carrier_fl_num",
    "origin",
    "origin_state_nm",
    "dest",
    "crs_dep_time",
    "dep_time",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "diverted",
    "distance",
]
PARQUET_COLUMNS = [column for column in CSV_COLUMNS if column != "origin_state_nm"]
REQUIRED_COLUMNS = {
    "fl_date",
    "origin",
    "dest",
    "op_unique_carrier",
    "op_carrier_fl_num",
    "crs_dep_time",
}


def _first_existing(paths: Iterable[Path]) -> Path | None:
    return next((path for path in paths if path.exists()), None)


def resolve_input_path(raw_path: str | None) -> Path:
    if raw_path:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"No existe la fuente indicada: {path}")
        return path

    env_path = os.environ.get("T1_FACT_PATH") or os.environ.get("FLIGHTS_DATA_PATH")
    candidates = [Path(env_path).expanduser()] if env_path else []
    candidates += [
        PROJECT_DIR / "data_clean" / "fact_flights",
        Path.cwd() / "data_clean" / "fact_flights",
        PROJECT_DIR.parent / "data_clean" / "fact_flights",
        Path("/home/lur/Documentos/archive/flight_data_2024.csv"),
        Path("/home/lur/Documentos/flight_data_2024.csv"),
        Path("/kaggle/input/flight-data-2024/flight_data_2024.csv"),
        Path.cwd() / "flight_data_2024.csv",
    ]
    path = _first_existing(candidates)
    if path is None:
        raise FileNotFoundError(
            "No encontré la fuente de Tarea 1. Pasa --input con la ruta a "
            "data_clean/fact_flights (Parquet) o a flight_data_2024.csv, "
            "o define T1_FACT_PATH."
        )
    return path.resolve()


def resolve_dimension_path(explicit_path: str | None, input_path: Path) -> Path | None:
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"No existe la dimensión indicada: {path}")
        return path

    env_path = os.environ.get("T1_DIM_PATH")
    candidates = [Path(env_path).expanduser()] if env_path else []
    candidates += [
        input_path.parent / "dim_airports",
        input_path.parent / "dim_airports.parquet",
        PROJECT_DIR / "data_clean" / "dim_airports",
        Path.cwd() / "data_clean" / "dim_airports",
        PROJECT_DIR.parent / "data_clean" / "dim_airports",
    ]
    return _first_existing(candidates)


def _load_arrow_modules() -> tuple[Any, Any, Any]:
    try:
        import pyarrow.csv as csv
        import pyarrow.dataset as dataset
        import pyarrow.parquet as parquet
    except ImportError as exc:
        raise RuntimeError(
            "Falta PyArrow. Activa el entorno del proyecto e instala "
            "requirements.txt (pip install -r requirements.txt)."
        ) from exc
    return csv, dataset, parquet


def load_airport_states(dimension_path: Path | None) -> dict[str, str]:
    if dimension_path is None:
        return {}
    _, dataset_module, _ = _load_arrow_modules()
    table = dataset_module.dataset(str(dimension_path), format="parquet").to_table(
        columns=["airport_code", "state_name"]
    )
    mapping: dict[str, str] = {}
    for row in table.to_pylist():
        code = _normal_text(row.get("airport_code")).upper()
        state = _normal_text(row.get("state_name"))
        if code and state:
            mapping[code] = state
    return mapping


def iter_source_rows(
    input_path: Path, batch_size: int
) -> Iterator[dict[str, Any]]:
    csv_module, dataset_module, _ = _load_arrow_modules()

    is_csv_file = input_path.is_file() and input_path.suffix.lower() in {".csv", ".txt"}
    if is_csv_file:
        reader = csv_module.open_csv(
            str(input_path),
            read_options=csv_module.ReadOptions(block_size=1 << 20, use_threads=True),
            convert_options=csv_module.ConvertOptions(include_columns=CSV_COLUMNS),
        )
        for batch in reader:
            yield from batch.to_pylist()
        return

    source = dataset_module.dataset(
        str(input_path), format="parquet", partitioning="hive"
    )
    source_columns = set(source.schema.names)
    missing = REQUIRED_COLUMNS - source_columns
    if missing:
        raise ValueError(
            f"Faltan columnas requeridas en {input_path}: {', '.join(sorted(missing))}"
        )
    selected_columns = [column for column in CSV_COLUMNS if column in source_columns]
    scanner = source.scanner(columns=selected_columns, batch_size=batch_size)
    for batch in scanner.to_batches():
        yield from batch.to_pylist()


def _normal_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _numeric(value: Any) -> float | None:
    if value is None or _normal_text(value) == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _number_as_text(value: Any) -> str:
    number = _numeric(value)
    if number is None:
        return _normal_text(value)
    if number.is_integer():
        return str(int(number))
    return str(number)


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _normal_text(value)
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def _hhmm(value: Any) -> tuple[datetime_time, int] | None:
    number = _numeric(value)
    if number is None:
        return None
    hhmm = int(number)
    hour, minute = divmod(hhmm, 100)
    if hour == 24 and minute == 0:
        return datetime_time(0, 0), 1
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return datetime_time(hour, minute), 0
    return None


def make_event_time(row: dict[str, Any]) -> tuple[datetime, str]:
    """Usa la salida programada como tiempo de evento.

    La hora programada existe para todos los vuelos, incluidos los cancelados.
    Si faltara, se usa la salida real como respaldo y, en último caso, la fecha
    del vuelo a medianoche.
    """
    flight_date = _date_value(row.get("fl_date"))
    scheduled_time = _hhmm(row.get("crs_dep_time"))

    if scheduled_time is not None:
        clock_time, day_offset = scheduled_time
        return datetime.combine(flight_date, clock_time) + timedelta(days=day_offset), "scheduled_departure_local"

    actual_time = _hhmm(row.get("dep_time"))
    if actual_time is not None:
        clock_time, day_offset = actual_time
        return datetime.combine(flight_date, clock_time) + timedelta(days=day_offset), "actual_departure_fallback"

    return datetime.combine(flight_date, datetime_time(0, 0)), "date_midnight_fallback"


def timestamp_ms(event_time: datetime) -> int:
    """Convierte hora local sin zona en epoch usando UTC como representación técnica."""
    return calendar.timegm(event_time.timetuple()) * 1000 + event_time.microsecond // 1000


def make_flight_id(row: dict[str, Any]) -> str:
    parts = [
        _date_value(row.get("fl_date")).isoformat(),
        _normal_text(row.get("op_unique_carrier")).upper() or "UNKNOWN",
        _number_as_text(row.get("op_carrier_fl_num")) or "UNKNOWN",
        _normal_text(row.get("origin")).upper() or "UNKNOWN",
        _number_as_text(row.get("crs_dep_time")) or "UNKNOWN",
    ]
    return "|".join(parts)


def make_payload(
    row: dict[str, Any],
    airport_states: dict[str, str],
    sequence: int,
    run_id: str,
) -> tuple[str, dict[str, Any], int]:
    flight_id = make_flight_id(row)
    origin = _normal_text(row.get("origin")).upper()
    event_time, event_time_source = make_event_time(row)
    state = _normal_text(row.get("origin_state_nm")) or airport_states.get(origin, "UNKNOWN")
    payload: dict[str, Any] = {
        "schema_version": 1,
        "event_type": "flight_observed",
        "event_id": flight_id,
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "event_time_source": event_time_source,
        "fl_date": _date_value(row.get("fl_date")).isoformat(),
        "origin": origin,
        "origin_state": state,
        "dest": _normal_text(row.get("dest")).upper(),
        "op_unique_carrier": _normal_text(row.get("op_unique_carrier")).upper(),
        "op_carrier_fl_num": _number_as_text(row.get("op_carrier_fl_num")),
        "dep_delay": _numeric(row.get("dep_delay")),
        "arr_delay": _numeric(row.get("arr_delay")),
        "cancelled": _numeric(row.get("cancelled")) or 0.0,
        "diverted": _numeric(row.get("diverted")) or 0.0,
        "distance": _numeric(row.get("distance")),
        "event_sequence": sequence,
        "producer_run_id": run_id,
        "ingested_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "test_injected_late": False,
    }
    return flight_id, payload, timestamp_ms(event_time)


def _delivery_callback(stats: dict[str, Any], event_id: str):
    def on_delivery(error: Any, message: Any) -> None:
        if error is not None:
            stats["failed"] += 1
            stats["last_failed_event"] = event_id
            stats["last_error"] = str(error)
            print(
                "DELIVERY_FAILURE="
                + json.dumps(
                    {
                        "event_id": event_id,
                        "attempt": stats.get("max_attempts"),
                        "error": str(error),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                file=sys.stderr,
            )
        else:
            stats["delivered"] += 1

    return on_delivery


def publish(
    producer: Any,
    topic: str,
    key: str,
    payload: dict[str, Any],
    event_timestamp_ms: int,
    on_delivery: Any,
) -> None:
    message = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    headers = [
        ("source", b"tarea1-us-flight-data-2024"),
        ("schema_version", b"1"),
        ("event_type", b"flight_observed"),
    ]
    while True:
        try:
            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=message,
                headers=headers,
                # El topic usa LogAppendTime; el broker reemplaza este timestamp
                # por la hora de append para que la retención no borre 2024.
                timestamp=event_timestamp_ms,
                on_delivery=on_delivery,
            )
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.1)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publica US Flight Data 2024 como eventos Kafka."
    )
    parser.add_argument("--input", help="CSV original o carpeta Parquet fact_flights")
    parser.add_argument(
        "--dimension-path",
        help="Carpeta Parquet dim_airports (necesaria si fact_flights no incluye estado)",
    )
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "flights.events"))
    parser.add_argument("--rate", type=float, default=100.0, help="Tasa objetivo (eventos/s); 0 = máximo")
    limit = parser.add_mutually_exclusive_group()
    limit.add_argument("--max-events", type=int, default=10_000, help="Límite para una prueba acotada (predeterminado: 10000)")
    limit.add_argument("--duration-seconds", type=float, help="Publica durante este tiempo; útil para el ensayo de carga")
    limit.add_argument("--all", action="store_true", help="Reproduce todas las filas de la fuente")
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--max-attempts", type=int, default=5, help="Intentos totales por mensaje (predeterminado: 5)")
    parser.add_argument("--summary-path", help="Ruta donde escribir el resumen JSON de la ejecución")
    parser.add_argument("--run-id", default=None, help="Identificador opcional de esta reproducción")
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()
    if args.rate < 0:
        raise SystemExit("--rate debe ser >= 0")
    if args.max_events is not None and args.max_events < 1:
        raise SystemExit("--max-events debe ser >= 1")
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        raise SystemExit("--duration-seconds debe ser > 0")
    if args.batch_size < 1:
        raise SystemExit("--batch-size debe ser >= 1")
    if args.max_attempts < 2:
        raise SystemExit("--max-attempts debe ser >= 2 para mantener la idempotencia")

    input_path = resolve_input_path(args.input)
    dimension_path = resolve_dimension_path(args.dimension_path, input_path)
    airport_states = load_airport_states(dimension_path)
    if input_path.is_dir() or input_path.suffix.lower() == ".parquet":
        _, dataset_module, _ = _load_arrow_modules()
        source_columns = set(
            dataset_module.dataset(
                str(input_path), format="parquet", partitioning="hive"
            ).schema.names
        )
        if "origin_state_nm" not in source_columns and not airport_states:
            raise RuntimeError(
                "fact_flights no contiene origin_state_nm y no encontré dim_airports; "
                "indica --dimension-path para poder agrupar por estado."
            )
    run_id = args.run_id or str(uuid.uuid4())
    event_limit = None if args.all or args.duration_seconds is not None else args.max_events

    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Falta confluent-kafka. Instala las dependencias con "
            "pip install -r requirements.txt."
        ) from exc

    stats: dict[str, Any] = {
        "delivered": 0,
        "failed": 0,
        "max_attempts": args.max_attempts,
        "last_failed_event": None,
        "last_error": None,
    }
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": f"tarea2-producer-{run_id[:8]}",
            "acks": "all",
            "enable.idempotence": True,
            "retries": args.max_attempts - 1,
            "linger.ms": 10,
        }
    )

    print(f"Fuente: {input_path}")
    print(f"Tópico: {args.topic} | broker(s): {args.bootstrap_servers}")
    print(f"Tasa objetivo: {args.rate:g} eventos/s | run_id: {run_id}")
    print(
        f"Reintentos configurados: {args.max_attempts} intentos totales "
        f"(1 envío + {args.max_attempts - 1} reintentos)"
    )
    if event_limit is not None:
        print(f"Límite de esta ejecución: {event_limit:,} eventos")

    attempted = 0
    started = time.monotonic()
    next_send_at = started
    deadline = (
        started + args.duration_seconds if args.duration_seconds is not None else None
    )
    aborted_by_delivery = False
    for row in iter_source_rows(input_path, args.batch_size):
        if event_limit is not None and attempted >= event_limit:
            break
        if deadline is not None and time.monotonic() >= deadline:
            break

        if args.rate > 0:
            now = time.monotonic()
            wait_seconds = next_send_at - now
            if wait_seconds > 0:
                time.sleep(wait_seconds)

        key, payload, kafka_timestamp = make_payload(
            row, airport_states, attempted, run_id
        )
        attempted += 1
        try:
            publish(
                producer,
                args.topic,
                key,
                payload,
                kafka_timestamp,
                _delivery_callback(stats, key),
            )
        except Exception as exc:
            stats["failed"] += 1
            stats["last_failed_event"] = key
            stats["last_error"] = f"produce: {exc}"
            print(f"ERROR al producir {key}: {exc}", file=sys.stderr)
            aborted_by_delivery = True
            break

        if args.rate > 0:
            # Límite estricto: el próximo envío se programa desde el instante
            # real en que terminó este, sin recuperar tiempo perdido.
            next_send_at = time.monotonic() + (1.0 / args.rate)

        if attempted % 10_000 == 0:
            print(f"Enviados al productor: {attempted:,}")

        if stats["failed"]:
            aborted_by_delivery = True
            break

    pending = producer.flush(10 if aborted_by_delivery else 60)
    if pending:
        print(
            "DELIVERY_PENDING="
            + json.dumps(
                {
                    "pending": pending,
                    "attempt": args.max_attempts,
                    "message": "quedaron mensajes sin confirmar tras flush",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
    elapsed = max(time.monotonic() - started, 1e-9)
    status = "ok" if not stats["failed"] and pending == 0 else "failed"
    summary = {
        "status": status,
        "run_id": run_id,
        "topic": args.topic,
        "source": str(input_path),
        "target_rate_events_per_second": args.rate,
        "pacing_mode": "strict" if args.rate > 0 else "unlimited",
        "max_attempts": args.max_attempts,
        "event_time": "scheduled_departure_local",
        "attempted": attempted,
        "delivered": stats["delivered"],
        "failed": stats["failed"],
        "pending_after_flush": pending,
        "failed_event_id": stats["last_failed_event"],
        "last_error": stats["last_error"],
        "elapsed_seconds": round(elapsed, 3),
        "delivered_events_per_second": round(stats["delivered"] / elapsed, 3),
    }
    if args.summary_path:
        summary_path = Path(args.summary_path).expanduser()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Resumen escrito en: {summary_path}")
    print("PRODUCER_SUMMARY=" + json.dumps(summary, ensure_ascii=False))
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        raise SystemExit(f"ERROR: {error}") from error
