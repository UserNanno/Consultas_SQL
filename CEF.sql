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
)

SELECT * FROM tasas;
