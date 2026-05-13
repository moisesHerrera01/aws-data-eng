-- =============================================================
-- EJERCICIO 05 — Totales acumulados y CTEs recursivas
-- Conceptos: SUM() OVER (ORDER BY), CTE recursiva, acumulados
-- =============================================================
--
-- PROBLEMA A: Para cada ruta, calcula el total acumulado de
-- pasajeros transportados día a día durante el mes de enero.
--
-- OBSERVACION:
--   SUM(x) OVER (PARTITION BY ... ORDER BY ...)
--   Sin ROWS/RANGE explícito, PostgreSQL usa por defecto
--   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW,
--   lo que acumula desde el inicio de la partición hasta la fila actual.
--   Para acumulados exactos en series densas, ROWS es más predecible.
-- =============================================================

-- PROBLEMA A: Acumulado de pasajeros por ruta
SELECT
    ruta,
    fecha,
    pasajeros,
    SUM(pasajeros) OVER (
        PARTITION BY ruta
        ORDER BY fecha
    )                              AS pasajeros_acumulados,
    SUM(pasajeros) OVER (
        PARTITION BY ruta
    )                              AS total_mes,
    ROUND(100.0 * SUM(pasajeros) OVER (PARTITION BY ruta ORDER BY fecha)
               / SUM(pasajeros) OVER (PARTITION BY ruta), 1)
                                   AS pct_del_total_alcanzado
FROM vuelos
WHERE estado != 'CANCELADO'
  AND DATE_TRUNC('month', fecha) = '2025-01-01'
ORDER BY ruta, fecha;

-- =============================================================
-- PROBLEMA B: Número de vuelos acumulados por estado (running count)
-- Muestra cuántos CANCELADOS han ocurrido hasta cada fecha
-- =============================================================
SELECT
    fecha,
    ruta,
    estado,
    COUNT(*) OVER (
        PARTITION BY ruta, estado
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                              AS conteo_acumulado_estado
FROM vuelos
WHERE fecha BETWEEN '2025-01-01' AND '2025-01-31'
  AND ruta = 'SCL-GRU'
ORDER BY fecha, estado;

-- =============================================================
-- PROBLEMA C: CTE recursiva — generar serie de fechas de operación
-- (útil para LEFT JOIN y detectar días sin vuelos)
-- =============================================================
WITH RECURSIVE serie_fechas AS (
    SELECT '2025-01-01'::date AS fecha
    UNION ALL
    SELECT fecha + 1
    FROM serie_fechas
    WHERE fecha < '2025-01-31'
)
SELECT
    s.fecha,
    COUNT(v.id)                    AS vuelos_del_dia,
    COALESCE(SUM(v.pasajeros), 0)  AS pasajeros_del_dia,
    CASE WHEN COUNT(v.id) = 0 THEN 'SIN OPERACION' ELSE 'OPERATIVO' END AS estado_dia
FROM serie_fechas s
LEFT JOIN vuelos v
    ON v.fecha = s.fecha
   AND v.ruta  = 'SCL-LIM'
   AND v.estado != 'CANCELADO'
GROUP BY s.fecha
ORDER BY s.fecha;
