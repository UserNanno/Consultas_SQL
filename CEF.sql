WITH base AS (
  SELECT
    CODMES,
    SEGMENTO,
    COUNT(*) AS total_solicitudes,
    SUM(FLGAPROBADO) AS total_aprobadas
  FROM T72496_HM_SOLICITUDES_SCRM
  GROUP BY CODMES, SEGMENTO
),

tasas AS (
  SELECT
    CODMES,
    SEGMENTO,
    total_solicitudes,
    total_aprobadas,
    total_aprobadas / total_solicitudes AS tasa_segmento,
    total_solicitudes / SUM(total_solicitudes) OVER (PARTITION BY CODMES) AS peso_segmento
  FROM base
),
t1 AS (
  SELECT * FROM tasas WHERE CODMES = '202601'
),
t2 AS (
  SELECT * FROM tasas WHERE CODMES = '202602'
)
SELECT
  SUM(efecto_mix) AS efecto_mix_total,
  SUM(efecto_performance) AS efecto_performance_total,
  SUM(efecto_interaccion) AS efecto_interaccion_total
FROM resultado;
ON t1.SEGMENTO = t2.SEGMENTO;
