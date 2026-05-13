-- =============================================================
-- EJERCICIO 04 — Comparación mes a mes de cancelaciones (MoM)
-- Conceptos: LAG(), variación porcentual, NULLIF
-- =============================================================
--
-- PROBLEMA:
-- Para cada ruta, muestra las cancelaciones de cada mes junto con
-- las del mes anterior y la variación porcentual.
-- Identifica qué rutas empeoraron más de un mes a otro.
--
-- PUNTO CLAVE:
--   LAG(expr, n, default) OVER (PARTITION BY ... ORDER BY ...)
--   El tercer argumento es el valor por defecto cuando no existe
--   la fila anterior (primer mes → NULL por defecto, 0 si se pone 0).
--   NULLIF(x, 0) evita división por cero al calcular el porcentaje.
-- =============================================================

WITH cancelaciones_mes AS (
    SELECT
        ruta,
        DATE_TRUNC('month', fecha)       AS mes,
        COUNT(*)                         AS cancelaciones
    FROM vuelos
    WHERE estado = 'CANCELADO'
    GROUP BY ruta, DATE_TRUNC('month', fecha)
),
con_lag AS (
    SELECT
        ruta,
        mes,
        cancelaciones,
        LAG(cancelaciones) OVER (
            PARTITION BY ruta
            ORDER BY mes
        )                                AS cancelaciones_mes_anterior,
        LAG(mes) OVER (
            PARTITION BY ruta
            ORDER BY mes
        )                                AS mes_anterior
    FROM cancelaciones_mes
)
SELECT
    ruta,
    TO_CHAR(mes, 'YYYY-MM')            AS mes,
    cancelaciones,
    cancelaciones_mes_anterior,
    CASE
        WHEN cancelaciones_mes_anterior IS NULL THEN NULL
        ELSE ROUND(
            100.0 * (cancelaciones - cancelaciones_mes_anterior)
            / NULLIF(cancelaciones_mes_anterior, 0),
        1)
    END                                AS variacion_pct,
    CASE
        WHEN cancelaciones > COALESCE(cancelaciones_mes_anterior, 0) THEN 'PEOR'
        WHEN cancelaciones < cancelaciones_mes_anterior              THEN 'MEJOR'
        ELSE 'IGUAL'
    END                                AS tendencia
FROM con_lag
ORDER BY ruta, mes;

-- =============================================================
-- VARIACIÓN: rutas que empeoraron más del 20% de un mes a otro
-- =============================================================
WITH cancelaciones_mes AS (
    SELECT
        ruta,
        DATE_TRUNC('month', fecha)       AS mes,
        COUNT(*)                         AS cancelaciones
    FROM vuelos
    WHERE estado = 'CANCELADO'
    GROUP BY ruta, DATE_TRUNC('month', fecha)
),
con_variacion AS (
    SELECT
        ruta,
        mes,
        cancelaciones,
        LAG(cancelaciones) OVER (PARTITION BY ruta ORDER BY mes) AS prev,
        ROUND(
            100.0 * (cancelaciones - LAG(cancelaciones) OVER (PARTITION BY ruta ORDER BY mes))
            / NULLIF(LAG(cancelaciones) OVER (PARTITION BY ruta ORDER BY mes), 0),
        1) AS variacion_pct
    FROM cancelaciones_mes
)
SELECT ruta, TO_CHAR(mes, 'YYYY-MM') AS mes, cancelaciones, prev AS mes_anterior, variacion_pct
FROM con_variacion
WHERE variacion_pct > 20
ORDER BY variacion_pct DESC;
