import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# En local se usan las variables de .env; en CI no existe y los tests de integración se saltan.
_env = ROOT / ".env"
if _env.exists():
    for line in _env.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
