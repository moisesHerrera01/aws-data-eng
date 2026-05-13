-- POC 06 — Seed Data
-- psql -h localhost -U pguser -d salesdb -f ddl/02_seed_data.sql

-- -------------------------------------------------------
-- rutas
-- -------------------------------------------------------
INSERT INTO rutas (codigo, origen, destino, distancia_km, aerolinea) VALUES
    ('SCL-LIM', 'SCL', 'LIM', 2453,  'LATAM'),
    ('SCL-BOG', 'SCL', 'BOG', 4651,  'LATAM'),
    ('SCL-MIA', 'SCL', 'MIA', 8657,  'LATAM'),
    ('BOG-MIA', 'BOG', 'MIA', 2777,  'Avianca'),
    ('LIM-BOG', 'LIM', 'BOG', 1875,  'Avianca'),
    ('MIA-JFK', 'MIA', 'JFK', 1757,  'American'),
    ('SCL-GRU', 'SCL', 'GRU', 2588,  'LATAM'),
    ('BOG-GRU', 'BOG', 'GRU', 4220,  'Avianca');

-- -------------------------------------------------------
-- vuelos — deterministic pattern using modular arithmetic
-- 8 routes x 120 days (Jan–Apr 2025) = 960 rows
--
-- Cancellation pattern by route index + day number:
--   ruta_idx 1 (SCL-LIM): cancels when (1 + day) % 8 = 0  → ~15/month
--   ruta_idx 3 (SCL-MIA): cancels when (3 + day) % 6 = 0  → more cancellations
--   ruta_idx 7 (SCL-GRU): cancels when (7 + day) % 5 = 0  → most cancellations
-- -------------------------------------------------------
INSERT INTO vuelos (ruta, fecha, estado, pasajeros, capacidad, aerolinea)
WITH rutas_list (ruta, ruta_idx, aerolinea) AS (
    VALUES
        ('SCL-LIM', 1, 'LATAM'),
        ('SCL-BOG', 2, 'LATAM'),
        ('SCL-MIA', 3, 'LATAM'),
        ('BOG-MIA', 4, 'Avianca'),
        ('LIM-BOG', 5, 'Avianca'),
        ('MIA-JFK', 6, 'American'),
        ('SCL-GRU', 7, 'LATAM'),
        ('BOG-GRU', 8, 'Avianca')
),
fechas AS (
    SELECT generate_series('2025-01-01'::date, '2025-04-30'::date, '1 day')::date AS fecha
),
base AS (
    SELECT
        r.ruta,
        r.ruta_idx,
        r.aerolinea,
        f.fecha,
        EXTRACT(DAY  FROM f.fecha)::int AS dia,
        EXTRACT(DOW  FROM f.fecha)::int AS dow,
        EXTRACT(MONTH FROM f.fecha)::int AS mes
    FROM rutas_list r CROSS JOIN fechas f
)
SELECT
    ruta,
    fecha,
    CASE
        -- SCL-GRU cancels the most (divisor 5)
        WHEN ruta_idx = 7 AND (ruta_idx + dia) % 5  = 0 THEN 'CANCELADO'
        -- SCL-MIA second most (divisor 6)
        WHEN ruta_idx = 3 AND (ruta_idx + dia) % 6  = 0 THEN 'CANCELADO'
        -- BOG-MIA third (divisor 6, different offset)
        WHEN ruta_idx = 4 AND (ruta_idx + dia) % 6  = 0 THEN 'CANCELADO'
        -- General cancellation (divisor 9, affects all routes less frequently)
        WHEN (ruta_idx + dia) % 9 = 0                   THEN 'CANCELADO'
        -- Delays (divisor 7)
        WHEN (ruta_idx * 2 + dia) % 7 = 0               THEN 'DEMORADO'
        ELSE 'COMPLETADO'
    END AS estado,
    -- Passengers: weekends fuller; varies by route index for variety
    CASE
        WHEN dow IN (5, 6) THEN 165 + (ruta_idx * 2)
        WHEN dow = 0       THEN 155 + (ruta_idx * 2)
        ELSE               130 + (ruta_idx * 3) + (dia % 10)
    END AS pasajeros,
    180 AS capacidad,
    aerolinea
FROM base;

-- -------------------------------------------------------
-- logs_sistema — events with deliberate gaps > 30 min
-- Normal cadence: every ~5 minutes
-- Gaps introduced: ~40 min, ~90 min, ~2h
-- -------------------------------------------------------
INSERT INTO logs_sistema (timestamp, sistema, severidad) VALUES
    ('2025-03-15 08:00:00', 'check-in',    'INFO'),
    ('2025-03-15 08:05:12', 'check-in',    'INFO'),
    ('2025-03-15 08:09:45', 'boarding',    'INFO'),
    ('2025-03-15 08:14:22', 'boarding',    'INFO'),
    ('2025-03-15 08:19:08', 'check-in',    'WARN'),
    -- GAP 1: 42 minutes (08:19 → 09:01)
    ('2025-03-15 09:01:33', 'check-in',    'ERROR'),
    ('2025-03-15 09:06:17', 'boarding',    'INFO'),
    ('2025-03-15 09:11:55', 'boarding',    'INFO'),
    ('2025-03-15 09:16:40', 'check-in',    'INFO'),
    ('2025-03-15 09:21:05', 'boarding',    'INFO'),
    ('2025-03-15 09:26:50', 'check-in',    'INFO'),
    -- GAP 2: 1h 37min (09:26 → 11:03)
    ('2025-03-15 11:03:22', 'check-in',    'ERROR'),
    ('2025-03-15 11:08:11', 'boarding',    'WARN'),
    ('2025-03-15 11:13:45', 'check-in',    'INFO'),
    ('2025-03-15 11:18:30', 'boarding',    'INFO'),
    ('2025-03-15 11:23:14', 'check-in',    'INFO'),
    -- GAP 3: 55 minutes (11:23 → 12:18)
    ('2025-03-15 12:18:05', 'check-in',    'INFO'),
    ('2025-03-15 12:23:41', 'boarding',    'INFO'),
    ('2025-03-15 12:28:19', 'check-in',    'INFO'),
    ('2025-03-15 12:33:02', 'boarding',    'INFO'),
    ('2025-03-15 12:38:55', 'check-in',    'INFO'),
    ('2025-03-15 12:44:10', 'boarding',    'INFO'),
    -- GAP 4: 2h 11min (12:44 → 14:55) — longest outage
    ('2025-03-15 14:55:00', 'check-in',    'ERROR'),
    ('2025-03-15 15:00:30', 'boarding',    'ERROR'),
    ('2025-03-15 15:05:45', 'check-in',    'WARN'),
    ('2025-03-15 15:10:22', 'boarding',    'INFO'),
    ('2025-03-15 15:15:08', 'check-in',    'INFO'),
    -- Small gap: 28 min (below threshold — should NOT appear in results)
    ('2025-03-15 15:43:00', 'boarding',    'INFO'),
    ('2025-03-15 15:48:15', 'check-in',    'INFO'),
    ('2025-03-15 15:53:40', 'boarding',    'INFO');

-- -------------------------------------------------------
-- tripulantes
-- -------------------------------------------------------
INSERT INTO tripulantes (nombre, cargo, base) VALUES
    ('Carlos Mendez',   'PILOTO',    'SCL'),
    ('Ana Rojas',       'PILOTO',    'SCL'),
    ('Diego Vargas',    'PILOTO',    'BOG'),
    ('Sofia Herrera',   'PILOTO',    'LIM'),
    ('Luis Paredes',    'PILOTO',    'MIA'),
    ('Maria Castro',    'COPILOTO',  'SCL'),
    ('Juan Torres',     'COPILOTO',  'SCL'),
    ('Paula Gomez',     'COPILOTO',  'BOG'),
    ('Andres Silva',    'COPILOTO',  'LIM'),
    ('Elena Rios',      'COPILOTO',  'MIA'),
    ('Carmen Lopez',    'SOBRECARGO','SCL'),
    ('Roberto Diaz',    'SOBRECARGO','SCL'),
    ('Valeria Mora',    'SOBRECARGO','BOG'),
    ('Felipe Nunez',    'SOBRECARGO','LIM'),
    ('Isabela Santos',  'SOBRECARGO','MIA');

-- -------------------------------------------------------
-- asignaciones_vuelo — assign crew to first 30 flights
-- -------------------------------------------------------
INSERT INTO asignaciones_vuelo (vuelo_id, tripulante_id)
SELECT v.id, t.id
FROM vuelos v
JOIN tripulantes t ON t.base = LEFT(v.ruta, 3)
WHERE v.id <= 30
  AND t.cargo IN ('PILOTO','COPILOTO');

SELECT
    'vuelos'       AS tabla, COUNT(*) AS filas FROM vuelos        UNION ALL
SELECT 'logs_sistema',               COUNT(*)  FROM logs_sistema  UNION ALL
SELECT 'tripulantes',                COUNT(*)  FROM tripulantes   UNION ALL
SELECT 'rutas',                      COUNT(*)  FROM rutas         UNION ALL
SELECT 'asignaciones_vuelo',         COUNT(*)  FROM asignaciones_vuelo;
