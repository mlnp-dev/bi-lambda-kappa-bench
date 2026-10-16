"""Convierte las hojas ASISTENCIA* del libro de marcaciones en eventos anonimizados.

Entrada : fuentes/MARCACIONES.xlsx   (datos personales, ignorado por Git)
Salida  : data/marcaciones.csv       (eventos sin datos personales, ignorado por Git)
          db/sedes.csv               (dimensión de sedes, sí se versiona)

Uso: python db/anonymize.py [ruta_xlsx]
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import hmac
import os
import sys
import unicodedata
import uuid
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
NS = uuid.UUID("6f1c2a52-3b7e-4d3a-9a51-6d0b6c1f0e01")  # espacio fijo para uuid5
TARDANZA_DESDE = dt.time(8, 0)  # supuesto: entrada posterior a las 08:00 = tardanza
FECHA_MIN, FECHA_MAX = dt.date(2019, 1, 1), dt.date(2021, 12, 31)  # alcance del estudio: 2019-2021
TZ = dt.timezone(dt.timedelta(hours=-5))  # hora de Perú, sin horario de verano

# Posición de columnas en las hojas ASISTENCIA* (fila 1 = cabecera)
C_SEDE, C_DNI, C_FECHA, C_NOMBRE = 3, 6, 7, 8
MARCAS = {9: "ENTRADA", 10: "SALIDA", 11: "REF_SALIDA", 12: "REF_RETORNO"}


def load_env() -> None:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    return " ".join(text.upper().split())


def as_time(v) -> dt.time | None:
    if v is None or v == "":
        return None
    if isinstance(v, dt.datetime):
        return v.time()
    if isinstance(v, dt.time):
        return v
    if isinstance(v, (int, float)) and 0 <= v < 1:  # fracción de día de Excel
        secs = round(v * 86400)
        return dt.time(secs // 3600 % 24, secs // 60 % 60)
    if isinstance(v, str):
        for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p"):
            try:
                return dt.datetime.strptime(v.strip(), fmt).time()
            except ValueError:
                pass
    return None


def person_key(dni, nombre) -> str | None:
    """DNI de 8 dígitos si existe; si no, el nombre normalizado."""
    if dni not in (None, ""):
        digits = "".join(ch for ch in str(dni).split(".")[0] if ch.isdigit())
        if digits and int(digits) > 0:
            return digits.zfill(8)
    if nombre and isinstance(nombre, str) and nombre.strip() and nombre != "No encontrado":
        return "N:" + norm(nombre)
    return None


def main(xlsx: Path) -> None:
    load_env()
    salt = os.environ.get("ANON_SALT", "").encode()
    if len(salt) < 16:
        sys.exit("Falta ANON_SALT en .env (mínimo 16 caracteres).")

    def staff_id(key: str) -> int:
        # 7 dígitos: suficiente para ~1 500 personas sin colisiones prácticas
        return int(hmac.new(salt, key.encode(), hashlib.sha256).hexdigest(), 16) % 9_000_000 + 1_000_000

    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    vistos: set[tuple[str, dt.date]] = set()
    sedes: dict[str, int] = {}
    eventos: list[tuple] = []
    stats = {"filas": 0, "duplicadas": 0, "sin_persona": 0, "sin_sede": 0, "fuera_rango": 0}

    for ws in wb.worksheets:
        if not ws.title.upper().startswith("ASIST"):
            continue
        rows = ws.iter_rows(values_only=True)
        header = next(rows, None)
        if not header or len(header) < 13 or norm(header[C_FECHA] or "") != "FECHA":
            continue
        for r in rows:
            if not r or len(r) < 13 or not isinstance(r[C_FECHA], dt.datetime):
                continue
            stats["filas"] += 1
            fecha = r[C_FECHA].date()
            # solo 2019-2021 (también descarta la hoja plantilla con fechas de 2026)
            if not (FECHA_MIN <= fecha <= FECHA_MAX):
                stats["fuera_rango"] += 1
                continue
            sede = norm(r[C_SEDE] or "")
            if not sede or sede.startswith("SELECCIONE"):
                stats["sin_sede"] += 1
                continue
            key = person_key(r[C_DNI], r[C_NOMBRE])
            if key is None:
                stats["sin_persona"] += 1
                continue
            if (key, fecha) in vistos:  # las hojas se solapan en fechas
                stats["duplicadas"] += 1
                continue
            vistos.add((key, fecha))
            sid = staff_id(key)
            sede_id = sedes.setdefault(sede, len(sedes) + 1)
            for col, tipo in MARCAS.items():
                t = as_time(r[col])
                if t is None:
                    continue
                ts = dt.datetime.combine(fecha, t, TZ)
                tard = tipo == "ENTRADA" and t > TARDANZA_DESDE
                eid = uuid.uuid5(NS, f"{sid}|{ts.isoformat()}|{tipo}")
                eventos.append((str(eid), sid, sede_id, tipo, tard, ts.isoformat()))

    eventos.sort(key=lambda e: e[5])
    (ROOT / "data").mkdir(exist_ok=True)
    with open(ROOT / "data" / "marcaciones.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "staff_id", "sede_id", "tipo", "tardanza", "event_ts"])
        w.writerows(eventos)
    with open(ROOT / "db" / "sedes.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sede_id", "nombre"])
        w.writerows(sorted((v, k) for k, v in sedes.items()))

    personas = len({e[1] for e in eventos})
    print(f"filas leídas={stats['filas']}  " + "  ".join(f"{k}={v}" for k, v in stats.items() if k != "filas"))
    print(f"eventos={len(eventos)}  personas={personas}  sedes={len(sedes)}  "
          f"rango={eventos[0][5][:10]}..{eventos[-1][5][:10]}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "fuentes" / "MARCACIONES.xlsx")
