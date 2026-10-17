-- Esquema del benchmark Lambda vs Kappa (PostgreSQL 16).
-- Se ejecuta una vez al crear el contenedor (docker-entrypoint-initdb.d).

-- Dimensión de sedes (no es dato personal). Se carga con `python tasks.py seed`.
CREATE TABLE sedes (
    sede_id  smallint PRIMARY KEY,
    nombre   text NOT NULL UNIQUE
);

-- Offsets de cada consumidor, guardados en la MISMA transacción que su escritura (exactly-once).
CREATE TABLE consumer_offsets (
    consumer     text     NOT NULL,   -- 'kappa_v1', 'kappa_v2', 'lambda_master', 'lambda_speed'
    partition    smallint NOT NULL,
    next_offset  bigint   NOT NULL,   -- próximo offset a leer
    PRIMARY KEY (consumer, partition)
);

-- Latencia de extremo a extremo por evento (solo con TRACK_LATENCY=1, para el benchmark).
CREATE TABLE e2e_latency (
    consumer      text        NOT NULL,
    produced_at   bigint      NOT NULL,   -- epoch ms del productor
    committed_at  timestamptz NOT NULL    -- escritura en la transacción, justo antes del commit
);

-- Forma común de todas las vistas agregadas: por sede y hora.
-- (Se usa como plantilla con CREATE TABLE ... (LIKE agg_template INCLUDING ALL).)
CREATE TABLE agg_template (
    sede_id      smallint    NOT NULL,
    bucket       timestamptz NOT NULL,   -- date_trunc('hour', event_ts)
    marcaciones  bigint      NOT NULL,
    tardanzas    bigint      NOT NULL,
    PRIMARY KEY (sede_id, bucket)
);

-------------------------------------------------------------------------------
-- KAPPA
-------------------------------------------------------------------------------
CREATE TABLE kappa_view_v1 (LIKE agg_template INCLUDING ALL);
CREATE VIEW kappa_serving AS SELECT * FROM kappa_view_v1;   -- promote.py la redirige a vN

-------------------------------------------------------------------------------
-- LAMBDA
-------------------------------------------------------------------------------
-- Master dataset: inmutable, solo inserción, idempotente por event_id.
CREATE TABLE master_events (
    event_id         uuid        PRIMARY KEY,
    staff_id         integer     NOT NULL,
    sede_id          smallint    NOT NULL,
    tipo             text        NOT NULL CHECK (tipo IN ('ENTRADA','SALIDA','REF_SALIDA','REF_RETORNO')),
    tardanza         boolean     NOT NULL,
    event_ts         timestamptz NOT NULL,
    produced_at      bigint      NOT NULL,
    kafka_partition  smallint    NOT NULL,
    kafka_offset     bigint      NOT NULL,
    committed_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX master_events_part_off ON master_events (kafka_partition, kafka_offset);

-- Batch layer: dos tablas (A/B); batch_layer.py escribe en la inactiva y cambia la vista.
CREATE TABLE batch_view_a (LIKE agg_template INCLUDING ALL);
CREATE TABLE batch_view_b (LIKE agg_template INCLUDING ALL);
CREATE VIEW batch_active AS SELECT * FROM batch_view_a;

-- Hasta qué offset (exclusivo) cubre la vista batch activa, por partición.
CREATE TABLE lambda_cutoff (
    partition      smallint PRIMARY KEY,
    cutoff_offset  bigint   NOT NULL DEFAULT 0
);
INSERT INTO lambda_cutoff (partition) VALUES (0), (1), (2);

-- Speed layer: incremental, por bloques de offset para poder purgar con exactitud.
CREATE TABLE speed_view (
    sede_id          smallint    NOT NULL,
    bucket           timestamptz NOT NULL,
    kafka_partition  smallint    NOT NULL,
    offset_block     bigint      NOT NULL,   -- kafka_offset / BLOCK_SIZE
    marcaciones      bigint      NOT NULL,
    tardanzas        bigint      NOT NULL,
    PRIMARY KEY (sede_id, bucket, kafka_partition, offset_block)
);

-- event_id ya contados por la speed layer (deduplicación); se purga junto con speed_view.
CREATE TABLE speed_seen (
    event_id         uuid     PRIMARY KEY,
    kafka_partition  smallint NOT NULL,
    offset_block     bigint   NOT NULL
);

-- Serving: batch + speed en cada consulta.
CREATE VIEW lambda_serving AS
SELECT sede_id, bucket, sum(marcaciones)::bigint AS marcaciones, sum(tardanzas)::bigint AS tardanzas
FROM (
    SELECT sede_id, bucket, marcaciones, tardanzas FROM batch_active
    UNION ALL
    SELECT sede_id, bucket, marcaciones, tardanzas FROM speed_view
) u
GROUP BY sede_id, bucket;
