# bi-lambda-kappa-bench

Benchmark de dos arquitecturas de ingesta (**Lambda** y **Kappa**) y dos arquitecturas de API
(**REST** y **GraphQL**) sobre marcaciones de asistencia **anonimizadas** del Ministerio Público,
Distrito Fiscal de Puno (2019 a junio de 2021: 218 841 eventos, 460 personas, 16 sedes).

Business Intelligence, maestría (FINESI). Actividad N° 1: Arquitecturas.

> Estado: fase 2 (Kappa) terminada. Lambda, APIs, benchmark e informe se agregan en las fases siguientes.

## Stack

PostgreSQL 16 · Redpanda (API de Kafka) · Python 3.11 · Docker Desktop (Windows 11).

## Arquitectura (Kappa)

```
data/marcaciones.csv ──producer──▶ Redpanda "marcaciones" (3 particiones, retención infinita)
                                          │
                                processor (kappa_vN) ──▶ kappa_view_vN ◀── kappa_serving (vista)
                                          │
                        replay desde offset 0 ──▶ kappa_view_v(N+1) ──▶ promoción
```

- **Exactly-once (efecto):** cada lote escribe el agregado, los `event_id` vistos y los offsets
  en una sola transacción de PostgreSQL. Al arrancar se leen los offsets de la BD y se hace `seek`;
  `enable.auto.commit = false`.
- **Idempotencia:** `event_id` es determinista (uuid5); `kappa_seen_vN` evita contar dos veces un
  evento reenviado.
- **Reproceso:** `replay` crea una versión nueva con su propio consumidor, lee el topic desde el
  offset 0 y, al alcanzar el final, cambia `kappa_serving` con `CREATE OR REPLACE VIEW`.

## Cómo levantarlo

```bash
cp .env.example .env              # y cambiar ANON_SALT y POSTGRES_PASSWORD
pip install -e ".[dev]"
python tasks.py up                # Postgres + Redpanda + topic con 3 particiones
python tasks.py seed              # dimensión de sedes
python tasks.py anon              # genera data/marcaciones.csv (requiere las fuentes locales)
```

## Kappa

```bash
python tasks.py produce                   # envía las 218 841 marcaciones al topic
python tasks.py kappa --until-caught-up   # procesa el backlog (sin la opción: modo continuo)
python tasks.py verify                    # las vistas deben cuadrar exactamente con el CSV
python tasks.py replay                    # replay completo a v(N+1) y promoción
```

## Tests

```bash
pytest -q
```

Los tests de integración (`-m integration`) usan un topic y un esquema de prueba propios y se
saltan si Postgres o Redpanda no están levantados (como en CI).

## Datos personales

Las fuentes (padrón y marcaciones) **nunca** se versionan. `staff_id` es un HMAC-SHA256 truncado
del DNI con una sal que solo existe en `.env`. Al repositorio no llegan nombres, DNI, celulares
ni sueldos.
