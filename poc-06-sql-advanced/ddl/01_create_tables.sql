-- POC 06 — Advanced SQL Practice (Aviation Domain)
-- Run against: PostgreSQL 15 (Docker) — database: salesdb
-- psql -h localhost -U pguser -d salesdb -f ddl/01_create_tables.sql

DROP TABLE IF EXISTS asignaciones_vuelo CASCADE;
DROP TABLE IF EXISTS tripulantes         CASCADE;
DROP TABLE IF EXISTS logs_sistema        CASCADE;
DROP TABLE IF EXISTS vuelos              CASCADE;
DROP TABLE IF EXISTS rutas               CASCADE;

-- -------------------------------------------------------
-- rutas — master table of flight routes
-- -------------------------------------------------------
CREATE TABLE rutas (
    ruta_id      SERIAL PRIMARY KEY,
    codigo       VARCHAR(10)  NOT NULL UNIQUE,  -- e.g. 'SCL-LIM'
    origen       VARCHAR(5)   NOT NULL,
    destino      VARCHAR(5)   NOT NULL,
    distancia_km INT          NOT NULL,
    aerolinea    VARCHAR(30)
);

-- -------------------------------------------------------
-- vuelos — one row per flight (daily frequency per route)
-- -------------------------------------------------------
CREATE TABLE vuelos (
    id         SERIAL PRIMARY KEY,
    ruta       VARCHAR(10)  NOT NULL,
    fecha      DATE         NOT NULL,
    estado     VARCHAR(15)  NOT NULL CHECK (estado IN ('COMPLETADO','CANCELADO','DEMORADO')),
    pasajeros  INT,
    capacidad  INT          NOT NULL DEFAULT 180,
    aerolinea  VARCHAR(30)
);

CREATE INDEX idx_vuelos_ruta_fecha ON vuelos (ruta, fecha);
CREATE INDEX idx_vuelos_estado     ON vuelos (estado);

-- -------------------------------------------------------
-- logs_sistema — system event log (for gap analysis)
-- -------------------------------------------------------
CREATE TABLE logs_sistema (
    id_evento  SERIAL PRIMARY KEY,
    timestamp  TIMESTAMP NOT NULL,
    sistema    VARCHAR(30),
    severidad  VARCHAR(10) CHECK (severidad IN ('INFO','WARN','ERROR'))
);

-- -------------------------------------------------------
-- tripulantes — crew members
-- -------------------------------------------------------
CREATE TABLE tripulantes (
    id      SERIAL PRIMARY KEY,
    nombre  VARCHAR(50)  NOT NULL,
    cargo   VARCHAR(15)  NOT NULL CHECK (cargo IN ('PILOTO','COPILOTO','SOBRECARGO')),
    base    VARCHAR(5)   NOT NULL
);

-- -------------------------------------------------------
-- asignaciones_vuelo — crew assignments per flight
-- -------------------------------------------------------
CREATE TABLE asignaciones_vuelo (
    vuelo_id      INT NOT NULL REFERENCES vuelos(id),
    tripulante_id INT NOT NULL REFERENCES tripulantes(id),
    PRIMARY KEY (vuelo_id, tripulante_id)
);
