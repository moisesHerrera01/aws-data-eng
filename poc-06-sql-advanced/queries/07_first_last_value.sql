-- =============================================================
-- EJERCICIO 07 — FIRST_VALUE / LAST_VALUE y comparación con baseline
-- Conceptos: FIRST_VALUE, LAST_VALUE, UNBOUNDED FOLLOWING, diferencia vs baseline
-- =============================================================
--
-- PROBLEMA A: Para cada vuelo de una ruta, muestra la ocupación
-- del primer y último vuelo del mes como referencia (baseline).
-- Calcula cuánto difiere cada día del baseline inicial.
--
-- OBSERVACION — LAST_VALUE necesita UNBOUNDED FOLLOWING:
--   Por defecto la ventana es RANGE UNBOUNDED PRECEDING TO CURRENT ROW.
--   LAST_VALUE con esa definición devuelve el valor de la fila actual,
--   no el verdadero último de la partición.
--   Usamos ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
--   para que la ventana cubra toda la partición.
-- =============================================================

SELECT
    ruta,
    fecha,
    ROUND(100.0 * pasajeros / capacidad, 2)           AS ocupacion_pct,

    ROUND(FIRST_VALUE(100.0 * pasajeros / capacidad) OVER (
        PARTITION BY ruta, DATE_TRUNC('month', fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 2)                                              AS ocupacion_primer_vuelo_mes,

    ROUND(LAST_VALUE(100.0 * pasajeros / capacidad) OVER (
        PARTITION BY ruta, DATE_TRUNC('month', fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 2)                                              AS ocupacion_ultimo_vuelo_mes,

    ROUND((100.0 * pasajeros / capacidad) - FIRST_VALUE(100.0 * pasajeros / capacidad) OVER (
        PARTITION BY ruta, DATE_TRUNC('month', fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 2)                                              AS diferencia_vs_primer_vuelo
FROM vuelos
WHERE estado != 'CANCELADO'
  AND ruta = 'SCL-LIM'
  AND DATE_TRUNC('month', fecha) = '2025-01-01'
ORDER BY fecha;

-- =============================================================
-- PROBLEMA B: NTH_VALUE — ocupación del 3er vuelo del mes
-- para comparar arranque vs estado en el día 3
-- =============================================================
SELECT
    ruta,
    fecha,
    ROUND(100.0 * pasajeros / capacidad, 2)           AS ocupacion_pct,
    ROUND(NTH_VALUE(100.0 * pasajeros / capacidad, 3) OVER (
        PARTITION BY ruta, DATE_TRUNC('month', fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 2)                                              AS ocupacion_dia_3_del_mes
FROM vuelos
WHERE estado != 'CANCELADO'
  AND DATE_TRUNC('month', fecha) = '2025-02-01'
ORDER BY ruta, fecha;

-- =============================================================
-- PROBLEMA C: NTILE — segmentar vuelos en cuartiles por ocupación
-- (útil para clasificar "vuelos llenos" vs "vuelos vacíos")
-- =============================================================
SELECT
    ruta,
    fecha,
    pasajeros,
    ROUND(100.0 * pasajeros / capacidad, 2)     AS ocupacion_pct,
    NTILE(4) OVER (ORDER BY pasajeros DESC)     AS cuartil,
    CASE NTILE(4) OVER (ORDER BY pasajeros DESC)
        WHEN 1 THEN 'LLENO'
        WHEN 2 THEN 'ALTO'
        WHEN 3 THEN 'MEDIO'
        WHEN 4 THEN 'BAJO'
    END                                         AS categoria_ocupacion
FROM vuelos
WHERE estado != 'CANCELADO'
  AND DATE_TRUNC('month', fecha) = '2025-01-01'
ORDER BY cuartil, fecha;
