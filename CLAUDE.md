# CLAUDE.md — Benchmark Lambda vs Kappa y REST vs GraphQL

> Coloca este archivo en la raíz del repositorio. Claude Code lo lee al iniciar cada sesión.
> Antes de empezar, completa la sección **0. Decisiones**.

## 0. Decisiones (completar antes de empezar)

| Parámetro | Valor |
|---|---|
| Base de datos | PostgreSQL 16 |
| Sistema operativo | Windows 11 + Docker Desktop (Ryzen 7 5700U, 16 GB RAM) |
| Lenguaje | Python 3.11 |
| Framework API | FastAPI (REST) y Strawberry (GraphQL) |
| Broker de eventos | Redpanda (compatible con Kafka) |
| Herramienta de carga | oha (o k6) |
| Informe | LaTeX, plantilla IEEEtran, máximo 2 páginas |
| Repositorio | `bi-lambda-kappa-bench` en GitHub, público |

## 1. Contexto de la tarea

Maestría, Business Intelligence and Data Analytics. Actividad N° 1 — Arquitecturas.

El docente pide:
1. Levantar **localmente** un servidor de base de datos y una API (opcional: exponerla en la nube o con un túnel).
2. Implementar una **plataforma de ingesta** con dos arquitecturas: **Lambda** y **Kappa**.
3. Implementar **dos arquitecturas de API**: **REST** y **GraphQL**.
4. Hacer un **benchmark de rendimiento** en cada caso y concluir cuál arquitectura resulta mejor para el caso de uso.
5. Entregables:
   - URL del repositorio en GitHub, con historial de versiones ordenado y las APIs visibles.
   - Informe del benchmark de **máximo 2 páginas** (2 arquitecturas de ingesta y 2 de API).
   - Referencias de **Scopus, ScienceDirect o IEEE**.

## 2. Caso de uso

Ingesta de **marcaciones de asistencia** reales del personal del Ministerio Público, Distrito Fiscal de Puno, **2019 a junio de 2021**, **anonimizadas**: 218 841 eventos (2019: 184 403; 2020: 29 426; 2021: 5 012), 460 personas, 16 sedes. 2020-2021 tienen menos registros (pandemia, trabajo remoto y uso de otros medios de registro de asistencia para evitar el contagio): se documenta en el informe.

Fuentes (nunca se versionan, están en `.gitignore`):
- `PERSONAL AL 12 DE AGOSTO DE 2026.xlsx`: padrón de personal (DNI, domicilio, sueldo, etc.).
- Hoja de Google «marcaciones» (hojas `ASISTENCIA*`): por persona y día, ENTRADA, SALIDA, REFRIGERIO salida y retorno.

`db/anonymize.py` convierte cada fila día-persona en hasta 4 eventos y deja `data/marcaciones.csv` (local, ignorado por Git). Al repositorio **no** llegan nombres, DNI, celulares ni sueldos: `staff_id` es un HMAC-SHA256 truncado del DNI con una sal guardada solo en `.env`.

```json
{
  "event_id": "uuid5(staff_id|event_ts|tipo)",
  "staff_id": 48213,
  "sede_id": 3,
  "tipo": "ENTRADA | SALIDA | REF_SALIDA | REF_RETORNO",
  "tardanza": false,
  "event_ts": "2019-04-02T08:05:00-05:00",
  "produced_at": 1792180000000
}
```

- `event_id` es determinista: reenviar el mismo archivo no duplica eventos (idempotencia).
- `tardanza` = ENTRADA posterior a las 08:00 (supuesto documentado en el informe).
- `event_ts` conserva la fecha y hora real de la marcación (zona -05:00); el `bucket` horario se calcula sobre ella.
- `produced_at` (epoch en ms) sirve para medir la latencia de extremo a extremo.
- La clave del mensaje en Kafka es `sede_id`.
- Agregado objetivo: por `sede_id` y hora (`bucket`), cantidad de marcaciones y cantidad de tardanzas.
- Dimensión `sedes` (Puno, Juliaca, Ilave, Azángaro, Melgar, …): no es dato personal y sí se versiona.

## 3. Arquitectura

```
Generador / POST /events
        │
        ▼
Redpanda — topic "payments", 3 particiones, retención infinita
        │
 ┌──────┴────────────────────────────┐
 │ LAMBDA                            │ KAPPA
 │ master sink → master_events       │ stream processor → kappa_view_vN
 │ batch layer → batch_view (A/B)    │ replay desde offset 0 → v(N+1)
 │ speed layer → speed_view          │ cambio de vista → kappa_serving
 │ lambda_serving = batch + speed    │
 └──────┬────────────────────────────┘
        ▼
   Base de datos  ←  API REST  /  API GraphQL  (parámetro arch = lambda | kappa)
```

### 3.1 Lambda
- **Master sink:** guarda cada evento tal cual en `master_events`, de solo inserción, con `event_id` único. Guarda también la partición y el offset de Kafka.
- **Batch layer:** recalcula todo el agregado desde `master_events` hasta un *cutoff* (offset por partición, alineado a bloques). Escribe en la tabla inactiva (A/B) y, en un solo paso atómico, cambia la vista activa, actualiza el *cutoff* y purga la capa speed.
- **Speed layer:** agrega de forma incremental los eventos posteriores al *cutoff*, por bloques de offset, para poder purgar con precisión.
- **Serving:** `lambda_serving` une batch y speed en cada consulta.

### 3.2 Kappa
- Un solo procesador de streaming que actualiza `kappa_view_v1`.
- **Reproceso:** un nuevo grupo de consumo lee desde el offset 0 hacia `kappa_view_v2`; al terminar, se cambia la vista `kappa_serving`.

### 3.3 Garantía de procesamiento (obligatorio en ambas)
- Los offsets se guardan **en la base de datos, dentro de la misma transacción que el agregado** (efecto *exactly-once*). Al arrancar, el consumidor lee esos offsets y hace `seek`.
- `enable.auto.commit = false`.
- Si la base de datos elegida no ofrece transacciones multi-documento o multi-tabla, aplica la adaptación de la sección 4 y documéntala en el informe.

## 4. Adaptación según la base de datos elegida

| Base de datos | Offsets y agregado atómicos | Cambio de vista (A/B, vN) | Observación |
|---|---|---|---|
| PostgreSQL / TimescaleDB | Transacción ACID | `CREATE OR REPLACE VIEW` | Opción de referencia; Timescale añade hypertables por tiempo |
| MySQL 8 | Transacción InnoDB | `CREATE OR REPLACE VIEW` | Upsert con `ON DUPLICATE KEY UPDATE` |
| MongoDB | Transacción multi-documento (requiere replica set, aunque sea de un nodo) | Colección de metadatos con el nombre de la colección activa | Upsert con `$inc` |
| ClickHouse | Sin transacciones: usar inserciones idempotentes (`ReplacingMergeTree` / `SummingMergeTree` con dedup por `event_id`) | `EXCHANGE TABLES` o vista | Muy rápida en agregación batch; latencia de consulta tras merges |
| Cassandra | Sin transacciones: contadores no idempotentes, usar tablas por `event_id` y agregar en consulta | Tabla de metadatos | Documentar la pérdida de garantías |

Regla: si una garantía no se puede cumplir, **no se oculta**; se documenta como limitación en el informe.

## 5. APIs

Las dos APIs comparten **el mismo módulo de repositorio** (mismas consultas). Solo cambia la capa de API.

| Operación | REST | GraphQL |
|---|---|---|
| Serie por hora | `GET /api/v1/sedes/{id}/hourly?from=&to=&arch=` | `query { hourly(sedeId, from, to, arch) }` |
| Ranking | `GET /api/v1/top-sedes?limit=&arch=` | `query { topSedes(limit, arch) }` |
| Resumen | `GET /api/v1/sedes/{id}/summary?arch=` | `query { summary(sedeId, arch) }` |
| Ingesta | `POST /api/v1/events` → Redpanda (202) | `mutation { ingestEvents(events) }` |
| Salud | `GET /health` | `GET /health` |

- `arch` solo acepta `lambda` o `kappa` (lista blanca; nunca concatenar SQL con texto del usuario).
- Un worker por API en el benchmark, para comparar en igualdad de condiciones.
- Documentación visible: `/docs` (Swagger) en REST y `/graphql` (GraphiQL) en GraphQL.

## 6. Estructura del repositorio

```
bi-lambda-kappa-bench/
├── CLAUDE.md
├── README.md                 # cómo levantar, cómo correr el benchmark, resultados
├── docker-compose.yml        # db, redpanda, procesadores, api-rest, api-graphql
├── Dockerfile
├── tasks.py                  # python tasks.py up/down/seed/lambda/kappa/batch/replay/bench/report (no hay make en Windows)
├── pyproject.toml
├── .env.example              # incluye ANON_SALT; .env real nunca se sube
├── data/                     # CSV anonimizado local (ignorado por Git)
├── db/                       # esquema e inicialización
├── src/lkbench/
│   ├── config.py  db.py  events.py  kafka.py
│   ├── producer.py           # generador de N eventos
│   ├── lambda_arch/          # master_sink.py, speed_layer.py, batch_layer.py
│   ├── kappa_arch/           # processor.py, promote.py
│   └── api/                  # repository.py, rest_app.py, graphql_app.py
├── bench/
│   ├── run_ingest.py         # throughput, latencia e2e, reproceso, consistencia
│   ├── run_api.py            # escenarios con oha/k6
│   ├── plot.py
│   └── results/              # JSON y CSV crudos + figuras
├── tests/
├── report/                   # informe.tex, refs.bib, figs/
└── .github/workflows/ci.yml  # ruff + pytest
```

## 7. Benchmark

### 7.1 Ingesta (Lambda frente a Kappa)
| Métrica | Cómo se mide |
|---|---|
| Throughput | Eventos procesados ÷ tiempo, con el backlog completo precargado (218 841 marcaciones reales 2019-2021; no se infla con datos sintéticos) |
| Latencia e2e p50, p95, p99 | Hora de commit en la BD − `produced_at`, con carga constante (por ejemplo 2 000 eventos/s), reemitiendo las marcaciones reales con `produced_at` actual |
| Frescura del dato | Lambda: speed layer e intervalo batch; Kappa: procesador |
| Tiempo de reproceso | Lambda: recálculo batch completo; Kappa: replay completo a vN+1 |
| Recursos | CPU y RAM con `docker stats` |
| Complejidad | Líneas de código y número de componentes que hay que mantener |
| Consistencia | Ambas vistas deben cuadrar exactamente con `master_events` |

### 7.2 API (REST frente a GraphQL, cruzado con vista Lambda y Kappa)
- Escenarios: `hourly`, `top-sedes` y `POST events`.
- Concurrencia de 10 y 50, 30 segundos por escenario y 10 segundos de calentamiento.
- Métricas: p50, p95, p99, requests/s y tasa de error.

### 7.3 Protocolo
- 3 repeticiones por escenario; se reporta la mediana.
- Misma máquina, sin otras cargas pesadas. Anotar CPU, RAM, sistema operativo y versiones.
- Todo reproducible con `python tasks.py bench`. Los resultados crudos se versionan en `bench/results/`.

## 8. Flujo de Git y GitHub

- Rama `main` protegida; cada fase en su rama y se integra por Pull Request.
- Ramas: `feat/infra`, `feat/kappa`, `feat/lambda`, `feat/api`, `bench/results`, `docs/informe`.
- Commits con *Conventional Commits*: `feat:`, `fix:`, `test:`, `bench:`, `docs:`, `chore:`.
- Tags por fase: `v0.1` … `v1.0`.
- Nunca subir `.env` ni credenciales; solo `.env.example`.

## 9. Fases de trabajo

| Fase | Rama | Entregable verificable | Tag |
|---|---|---|---|
| 1 | `feat/infra` | `python tasks.py up` levanta BD y Redpanda; esquema creado; topic con 3 particiones | v0.1 |
| 2 | `feat/kappa` | Generador y procesador Kappa; prueba de consistencia en verde; replay a v2 | v0.2 |
| 3 | `feat/lambda` | Master sink, speed layer y batch A/B; `lambda_serving` cuadra con `master_events` | v0.3 |
| 4 | `feat/api` | REST y GraphQL con las mismas operaciones; tests de contrato | v0.4 |
| 5 | `bench/results` | `python tasks.py bench` genera JSON, CSV y figuras en `bench/results/` | v0.5 |
| 6 | `docs/informe` | `report/informe.pdf` de 2 páginas; README final; repo publicado | v1.0 |

Al terminar cada fase: correr tests, hacer commit, abrir PR, fusionar y crear el tag.

## 10. Informe (LaTeX IEEEtran, máximo 2 páginas)

1. **Introducción:** problema y objetivo.
2. **Método:** entorno (hardware y versiones), caso de uso, arquitecturas y métricas.
3. **Resultados:** una tabla de ingesta, una tabla de API y una figura.
4. **Discusión:** contraste con la literatura y amenazas a la validez (un solo nodo, Python como cuello de botella, datos sintéticos).
5. **Conclusión:** qué arquitectura conviene y en qué condiciones. Incluir la URL del repositorio.

Referencias:
- Mínimo 4, de Scopus, ScienceDirect o IEEE Xplore.
- **Cada DOI se verifica en línea antes de citarlo.** No inventar referencias ni datos.
- Candidatas a verificar:
  - Lin, J. (2017). *The Lambda and the Kappa*. IEEE Internet Computing.
  - Persico, V. et al. (2018). Benchmark de arquitecturas Lambda y Kappa en la nube. *Future Generation Computer Systems*.
  - Brito, G. & Valente, M. T. (2020). *REST vs GraphQL: A Controlled Experiment*. IEEE ICSA.

Las cifras del informe salen **solo** de `bench/results/`; nada se escribe a mano.

## 11. Reglas de ingeniería

- ACID e idempotencia en la ingesta; YAGNI y KISS (nada de Spark, Flink ni Kubernetes); SOLID en la separación repositorio y API.
- Si dos principios entran en tensión (por ejemplo, exactitud frente a rendimiento), señalarlo y documentar la decisión.
- Configuración solo por variables de entorno.
- Consultas siempre parametrizadas.
- Toda afirmación del informe se respalda con un resultado del repositorio o con una referencia verificada.
