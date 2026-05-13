-- =============================================================
-- EJERCICIO 06 — ROW_NUMBER para deduplicación y "latest record"
-- Conceptos: ROW_NUMBER(), CTE + filtro, patrón dedup clásico
-- =============================================================
--
-- PROBLEMA A: Simula una tabla con duplicados y quédate solo con
-- el vuelo más reciente por ruta (patrón muy frecuente en ETL/CDC).
-- Con los datos actuales: para cada ruta, el último vuelo del mes.
--
-- PUNTO CLAVE — patrón "latest per group":
--   Este es uno de los patrones SQL más preguntados en entrevistas.
--   ROW_NUMBER() + CTE + WHERE rn = 1 es la forma canónica.
--   Alternativas: DISTINCT ON (solo PostgreSQL), subquery con MAX.
-- =============================================================

-- PROBLEMA A: Último vuelo registrado por ruta en cada mes
WITH numerados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ruta, DATE_TRUNC('month', fecha)
            ORDER BY fecha DESC
        ) AS rn
    FROM vuelos
)
SELECT
    ruta,
    TO_CHAR(fecha, 'YYYY-MM')   AS mes,
    fecha                       AS ultimo_vuelo,
    estado,
    pasajeros
FROM numerados
WHERE rn = 1
ORDER BY ruta, mes;

-- =============================================================
-- PROBLEMA B: Identificar el primer y último vuelo cancelado
-- de cada ruta — útil para auditorías de calidad de servicio
-- =============================================================
WITH cancelados_numerados AS (
    SELECT
        ruta,
        fecha,
        ROW_NUMBER() OVER (PARTITION BY ruta ORDER BY fecha ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY ruta ORDER BY fecha DESC) AS rn_desc
    FROM vuelos
    WHERE estado = 'CANCELADO'
)
SELECT
    ruta,
    MAX(fecha) FILTER (WHERE rn_asc  = 1) AS primera_cancelacion,
    MAX(fecha) FILTER (WHERE rn_desc = 1) AS ultima_cancelacion
FROM cancelados_numerados
GROUP BY ruta
ORDER BY ruta;

-- =============================================================
-- PROBLEMA C (PostgreSQL-específico): DISTINCT ON
-- Equivalente a ROW_NUMBER rn=1 pero más conciso en Postgres.
-- No disponible en otros motores (MySQL, SQL Server, BigQuery).
-- =============================================================
SELECT DISTINCT ON (ruta)
    ruta,
    fecha                   AS ultimo_vuelo,
    estado,
    pasajeros
FROM vuelos
ORDER BY ruta, fecha DESC;
-- NOTA: DISTINCT ON (col) mantiene la primera fila después del ORDER BY.
-- Es idiomático en Postgres pero no portable — saber ambas formas.
