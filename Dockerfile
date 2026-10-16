FROM python:3.11.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
RUN pip install .

# Sin datos ni secretos en la imagen: la configuración llega por variables de entorno.
USER nobody
CMD ["uvicorn", "--factory", "lkbench.api.rest_app:create_app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
