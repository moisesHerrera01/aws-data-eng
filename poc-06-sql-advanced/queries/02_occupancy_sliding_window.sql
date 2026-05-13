-- =============================================================
-- EJERCICIO 02 — Ocupación promedio con ventana deslizante 7 días
-- Conceptos: ROWS BETWEEN, ventana deslizante, división entera
-- =============================================================
--
-- PROBLEMA:
-- Calcula el % de ocupación diaria y el promedio móvil de los
-- últimos 7 días para cada ruta.
--
-- OBSERVACION — ROWS vs RANGE:
--   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
--     → ventana física: exactamente 6 filas anteriores + fila actual
--       independientemente de los valores de fecha
--   RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
--     → ventana lógica: filas cuya fecha esté dentro de 6 días
--       HACIA ATRÁS (maneja huecos en la serie de fechas)
-- Usamos ROWS cuando la serie es densa (una fila por día).
-- Usamos RANGE cuando la serie puede tener huecos de fechas.
-- =============================================================

SELECT
    ruta,
    fecha,
    pasajeros,
    capacidad,
    ROUND(100.0 * pasajeros / capacidad, 2)                        AS ocupacion_pct,
    ROUND(AVG(100.0 * pasajeros / capacidad) OVER (
        PARTITION BY ruta
        ORDER BY fecha
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                          AS ocupacion_promedio_7d,
    COUNT(*) OVER (
        PARTITION BY ruta
        ORDER BY fecha
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                                              AS dias_en_ventana
FROM vuelos
WHERE estado != 'CANCELADO'  -- vuelos cancelados no tienen pasajeros reales
ORDER BY ruta, fecha;

-- =============================================================
-- VARIACIÓN: agregar también min y max de ocupación en la ventana
-- (útil para detectar picos dentro del período)
-- =============================================================
SELECT
    ruta,
    fecha,
    ROUND(100.0 * pasajeros / capacidad, 2)                        AS ocupacion_pct,
    ROUND(AVG(100.0 * pasajeros / capacidad) OVER w, 2)           AS avg_7d,
    ROUND(MIN(100.0 * pasajeros / capacidad) OVER w, 2)           AS min_7d,
    ROUND(MAX(100.0 * pasajeros / capacidad) OVER w, 2)           AS max_7d
FROM vuelos
WHERE estado != 'CANCELADO'
WINDOW w AS (
    PARTITION BY ruta
    ORDER BY fecha
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
ORDER BY ruta, fecha;
-- NOTA: la cláusula WINDOW nombra la ventana una sola vez
-- y la reutiliza — evita repetición cuando usas la misma ventana en múltiples funciones.
