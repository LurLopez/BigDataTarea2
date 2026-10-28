#!/usr/bin/env python3
"""Empaqueta todas las evidencias de la Tarea 2 en un solo ZIP."""

from __future__ import annotations

import shutil
from pathlib import Path

BASE = Path("/content/tarea2_bigdata")
OUTPUTS = BASE / "outputs"
STAGING = Path("/content/evidencia_tarea2_4.2_4.6")
ZIP_BASE = Path("/content/evidencia_tarea2_4.2_4.6")

EVIDENCE = [
    "4.2_producer",
    "4.3_ingest",
    "4.4_queries",
    "4.5_capacity",
    "4.6_recovery",
]

existing = [OUTPUTS / name for name in EVIDENCE if (OUTPUTS / name).exists()]
missing = [name for name in EVIDENCE if not (OUTPUTS / name).exists()]

print("Evidencias encontradas:")
for path in existing:
    print("  OK", path)
if missing:
    print("Evidencias que faltan:")
    for name in missing:
        print("  FALTA", OUTPUTS / name)

if not existing:
    raise FileNotFoundError(
        "No hay evidencias en /content/tarea2_bigdata/outputs. "
        "Ejecuta las secciones 4.2–4.6 antes de descargar."
    )

if STAGING.exists():
    shutil.rmtree(STAGING)
STAGING.mkdir(parents=True, exist_ok=True)
for source in existing:
    shutil.copytree(source, STAGING / source.name)

zip_path = shutil.make_archive(str(ZIP_BASE), "zip", STAGING.parent, STAGING.name)
print(f"\nZIP creado: {zip_path}")
print(f"Tamaño: {Path(zip_path).stat().st_size / (1024 ** 2):.2f} MB")

try:
    from google.colab import files
    from IPython.display import FileLink, display
    files.download(zip_path)
    display(FileLink(zip_path))
except ImportError:
    print("No estás en Colab: el ZIP queda en disco.")
