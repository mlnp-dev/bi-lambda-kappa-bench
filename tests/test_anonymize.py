import datetime as dt
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location("anonymize", Path(__file__).parents[1] / "db" / "anonymize.py")
anonymize = importlib.util.module_from_spec(spec)
spec.loader.exec_module(anonymize)


def test_person_key_usa_dni_con_ceros():
    assert anonymize.person_key(1234567, "X") == "01234567"
    assert anonymize.person_key("01234567", None) == "01234567"


def test_person_key_sin_dni_usa_nombre_normalizado():
    assert anonymize.person_key(None, "Pérez  Quispe, Ana") == "N:PEREZ QUISPE, ANA"
    assert anonymize.person_key(None, "No encontrado") is None


def test_as_time_formatos():
    assert anonymize.as_time(dt.time(8, 5)) == dt.time(8, 5)
    assert anonymize.as_time(0.5) == dt.time(12, 0)
    assert anonymize.as_time("17:45") == dt.time(17, 45)
    assert anonymize.as_time(None) is None


def test_csv_sin_datos_personales():
    """Si el CSV ya fue generado, sus columnas no incluyen DNI, nombres ni celulares."""
    csv_path = Path(__file__).parents[1] / "data" / "marcaciones.csv"
    if csv_path.exists():
        header = csv_path.open(encoding="utf-8").readline().strip().split(",")
        assert header == ["event_id", "staff_id", "sede_id", "tipo", "tardanza", "event_ts"]
