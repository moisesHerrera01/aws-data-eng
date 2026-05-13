-- =============================================================
-- EJERCICIO 01 — Top 3 rutas con más cancelaciones por mes
-- Conceptos: CTE, GROUP BY, RANK() OVER (PARTITION BY ...)
-- =============================================================
--
-- PROBLEMA:
-- Obtén las 3 rutas con más cancelaciones para cada mes.
-- Si dos rutas empatan, ambas deben aparecer (no recortar empates).
--
-- OBSERVACION — RANK vs ROW_NUMBER:
--   ROW_NUMBER : numera sin importar empates (siempre 1,2,3,4...)
--   RANK       : si dos filas empatan en posición 2, ambas son 2
--                y la siguiente es 4 (no 3)
--   DENSE_RANK : igual que RANK pero sin saltar (2,2,3)
-- Usamos RANK (no ROW_NUMBER) para que rutas con igual número de
-- cancelaciones reciban el mismo ranking — sin recortar empates.
-- =============================================================

WITH cancelaciones AS (
    SELECT
        ruta,
        DATE_TRUNC('month', fecha)      AS mes,
        COUNT(*)                        AS total_cancelaciones
    FROM vuelos
    WHERE estado = 'CANCELADO'
    GROUP BY ruta, DATE_TRUNC('month', fecha)
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY mes
            ORDER BY total_cancelaciones DESC
        ) AS ranking
    FROM cancelaciones
)
SELECT
    TO_CHAR(mes, 'YYYY-MM') AS mes,
    ruta,
    total_cancelaciones,
    ranking
FROM ranked
WHERE ranking <= 3
ORDER BY mes, ranking;

-- =============================================================
-- VARIACIÓN: usa DENSE_RANK y compara resultados
-- (útil cuando el enrevistador pide "sin saltos en el ranking")
-- =============================================================
WITH cancelaciones AS (
    SELECT
        ruta,
        DATE_TRUNC('month', fecha)      AS mes,
        COUNT(*)                        AS total_cancelaciones
    FROM vuelos
    WHERE estado = 'CANCELADO'
    GROUP BY ruta, DATE_TRUNC('month', fecha)
),
ranked AS (
    SELECT
        *,
        RANK()       OVER (PARTITION BY mes ORDER BY total_cancelaciones DESC) AS rank,
        DENSE_RANK() OVER (PARTITION BY mes ORDER BY total_cancelaciones DESC) AS dense_rank,
        ROW_NUMBER() OVER (PARTITION BY mes ORDER BY total_cancelaciones DESC) AS row_num
    FROM cancelaciones
)
SELECT
    TO_CHAR(mes, 'YYYY-MM') AS mes,
    ruta,
    total_cancelaciones,
    rank,
    dense_rank,
    row_num
FROM ranked
ORDER BY mes, rank;
