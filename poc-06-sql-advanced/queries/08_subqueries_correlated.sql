-- =============================================================
-- EJERCICIO 08 — Subqueries correlacionadas y EXISTS / NOT EXISTS
-- Conceptos: subquery correlacionada, EXISTS, scalar subquery, IN vs EXISTS
-- =============================================================
--
-- PROBLEMA A: Para cada ruta, muestra si tuvo algún día con 100%
-- de cancelaciones (todos los vuelos del día cancelados).
-- Usa subquery correlacionada con EXISTS.
--
-- OBSERVACION — IN vs EXISTS vs JOIN:
--   IN       : evalúa toda la lista antes de comparar (sin cortocircuito)
--   EXISTS   : se detiene en el primer match — más eficiente con índices
--   NOT IN   : si la subquery puede devolver NULL, el resultado es siempre
--              FALSE → usamos NOT EXISTS para evitar ese comportamiento
--   JOIN     : equivalente a EXISTS pero puede duplicar filas en relaciones 1:N
-- =============================================================

-- PROBLEMA A: Rutas con al menos un día donde todos los vuelos se cancelaron
-- (escenario teórico — con datos actuales habrá 1 vuelo por día por ruta)
SELECT DISTINCT v.ruta
FROM vuelos v
WHERE EXISTS (
    SELECT 1
    FROM vuelos inner_v
    WHERE inner_v.ruta  = v.ruta
      AND inner_v.fecha = v.fecha
      AND inner_v.estado = 'CANCELADO'
      AND NOT EXISTS (
          SELECT 1
          FROM vuelos comp
          WHERE comp.ruta  = inner_v.ruta
            AND comp.fecha = inner_v.fecha
            AND comp.estado != 'CANCELADO'
      )
)
ORDER BY v.ruta;

-- =============================================================
-- PROBLEMA B: Scalar subquery — para cada vuelo, muestra cuántos
-- vuelos cancelados tuvo esa misma ruta en los 7 días anteriores
-- =============================================================
SELECT
    v.ruta,
    v.fecha,
    v.estado,
    (
        SELECT COUNT(*)
        FROM vuelos hist
        WHERE hist.ruta   = v.ruta
          AND hist.fecha  >= v.fecha - INTERVAL '7 days'
          AND hist.fecha  <  v.fecha
          AND hist.estado = 'CANCELADO'
    )                               AS cancelaciones_7d_previas
FROM vuelos v
WHERE v.ruta  = 'SCL-GRU'
  AND v.fecha BETWEEN '2025-01-08' AND '2025-01-31'
ORDER BY v.fecha;

-- =============================================================
-- PROBLEMA C: Rutas cuya tasa de cancelación supera el promedio
-- global — subquery en el WHERE
-- =============================================================
SELECT
    ruta,
    COUNT(*)                                                AS total_vuelos,
    COUNT(*) FILTER (WHERE estado = 'CANCELADO')           AS cancelados,
    ROUND(100.0 * COUNT(*) FILTER (WHERE estado = 'CANCELADO') / COUNT(*), 2)
                                                           AS tasa_cancelacion_pct
FROM vuelos
GROUP BY ruta
HAVING
    100.0 * COUNT(*) FILTER (WHERE estado = 'CANCELADO') / COUNT(*)
    > (
        SELECT 100.0 * COUNT(*) FILTER (WHERE estado = 'CANCELADO') / COUNT(*)
        FROM vuelos
    )
ORDER BY tasa_cancelacion_pct DESC;

-- =============================================================
-- PROBLEMA D: NOT EXISTS clásico — rutas que NUNCA tuvieron
-- un vuelo completado en febrero (no usar NOT IN: NULL trap)
-- =============================================================
SELECT DISTINCT ruta
FROM vuelos v
WHERE DATE_TRUNC('month', fecha) = '2025-02-01'
  AND NOT EXISTS (
      SELECT 1
      FROM vuelos comp
      WHERE comp.ruta   = v.ruta
        AND comp.estado = 'COMPLETADO'
        AND DATE_TRUNC('month', comp.fecha) = '2025-02-01'
  )
ORDER BY ruta;
