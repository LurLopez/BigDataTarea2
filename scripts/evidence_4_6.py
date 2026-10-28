#!/usr/bin/env python3
"""Empaqueta y descarga la evidencia de 4.6.

Uso en Colab:
    python scripts/evidence_4_6.py

También se puede ejecutar como celda copiando el contenido en el notebook.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

BASE = Path(os.environ.get("T2_46_BASE", "/content/tarea2_bigdata"))
SRC = BASE / "outputs" / "4.6_recovery"
ZIP_BASE = Path(os.environ.get("T2_46_ZIP", "/content/evidencia_4_6"))

print(f"Evidencia esperada: {SRC}")
if not SRC.exists():
    raise FileNotFoundError(
        f"No existe {SRC}. Ejecuta 4.6 completo antes de empaquetar."
    )

checks_path = SRC / "checks.json"
if checks_path.exists():
    checks = json.loads(checks_path.read_text(encoding="utf-8"))
    print("Checks de 4.6:")
    for name, ok in checks.items():
        print(f"  {name}: {'OK' if ok else 'FALLA'}")
    if not all(checks.values()):
        print(
            "\nADVERTENCIA: 4.6 no tiene todos los checks en true. "
            "El ZIP se genera igual para poder revisar el fallo."
        )
else:
    print("ADVERTENCIA: no existe checks.json.")

zip_path = shutil.make_archive(str(ZIP_BASE), "zip", SRC.parent, SRC.name)
print(f"ZIP creado: {zip_path}")
print(f"Tamaño: {Path(zip_path).stat().st_size / (1024 ** 2):.2f} MB")

try:
    from google.colab import files
    files.download(zip_path)
except ImportError:
    print("No estás en Colab: el ZIP queda en disco.")
