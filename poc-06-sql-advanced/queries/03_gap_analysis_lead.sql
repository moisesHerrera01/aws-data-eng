-- =============================================================
-- EJERCICIO 03 — Detección de brechas (gap analysis) con LEAD
-- Conceptos: LEAD(), INTERVAL, subquery vs CTE
-- =============================================================
--
-- PROBLEMA:
-- Encuentra intervalos donde el sistema estuvo inactivo más de
-- 30 minutos (sin ningún evento registrado).
--
-- OBSERVACION — LAG vs LEAD:
--   LAG (col, n)  → valor de n filas HACIA ATRÁS (fila anterior)
--   LEAD(col, n)  → valor de n filas HACIA ADELANTE (fila siguiente)
-- Usamos LEAD en gap analysis para comparar cada evento con
-- el SIGUIENTE evento y medir el intervalo entre ellos.
-- =============================================================

SELECT
    timestamp                                                       AS inicio_brecha,
    siguiente_evento                                                AS fin_brecha,
    sistema,
    EXTRACT(EPOCH FROM (siguiente_evento - timestamp)) / 60        AS minutos_sin_actividad
FROM (
    SELECT
        timestamp,
        sistema,
        LEAD(timestamp) OVER (ORDER BY timestamp)                   AS siguiente_evento
    FROM logs_sistema
) sub
WHERE siguiente_evento - timestamp > INTERVAL '30 minutes'
ORDER BY inicio_brecha;

-- =============================================================
-- VARIACIÓN 1: con CTE (más legible, equivalente en rendimiento)
-- =============================================================
WITH eventos_con_siguiente AS (
    SELECT
        id_evento,
        timestamp,
        sistema,
        severidad,
        LEAD(timestamp)  OVER (ORDER BY timestamp) AS siguiente_evento,
        LEAD(id_evento)  OVER (ORDER BY timestamp) AS siguiente_id
    FROM logs_sistema
)
SELECT
    id_evento                                              AS evento_inicio,
    siguiente_id                                           AS evento_fin,
    sistema,
    TO_CHAR(timestamp,        'HH24:MI:SS')               AS hora_inicio,
    TO_CHAR(siguiente_evento, 'HH24:MI:SS')               AS hora_fin,
    ROUND(EXTRACT(EPOCH FROM (siguiente_evento - timestamp)) / 60, 1) AS minutos_inactivo
FROM eventos_con_siguiente
WHERE siguiente_evento - timestamp > INTERVAL '30 minutes'
ORDER BY timestamp;

-- =============================================================
-- VARIACIÓN 2: brecha máxima y total de tiempo sin actividad
-- =============================================================
WITH brechas AS (
    SELECT
        LEAD(timestamp) OVER (ORDER BY timestamp) - timestamp AS duracion
    FROM logs_sistema
)
SELECT
    COUNT(*)                                                       AS total_brechas_30min,
    ROUND(MAX(EXTRACT(EPOCH FROM duracion)) / 60, 1)              AS brecha_maxima_min,
    ROUND(SUM(EXTRACT(EPOCH FROM duracion))
          FILTER (WHERE duracion > INTERVAL '30 minutes') / 60, 1) AS minutos_totales_sin_actividad
FROM brechas
WHERE duracion > INTERVAL '30 minutes';
