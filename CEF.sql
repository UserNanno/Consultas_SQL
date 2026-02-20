-- Parametriza periodos
WITH params AS (
  SELECT '202401' AS p1, '202402' AS p2
),

-- Lista de dimensiones a evaluar (driver candidates)
dims AS (
  SELECT 'SEGMENTO' AS dim_name UNION ALL
  SELECT 'CANAL' UNION ALL
  SELECT 'DESCAMPANIA' UNION ALL
  SELECT 'BRACKETATENCIONANALISTA' UNION ALL
  SELECT 'BRACKETDERIVACIONANALISTA' UNION ALL
  SELECT 'NIVELAUTONOMIA' UNION ALL
  SELECT 'DESTIPRENTA' UNION ALL
  SELECT 'PRODUCTO' UNION ALL
  SELECT 'DESPRODUCTO'
),

-- Helper: genera una vista "larga" (dim_name, dim_value) para poder iterar en SQL
long_base AS (
  SELECT
    t.CODMES,
    d.dim_name,
    CASE d.dim_name
      WHEN 'SEGMENTO' THEN COALESCE(t.SEGMENTO,'(NULL)')
      WHEN 'CANAL' THEN COALESCE(t.CANAL,'(NULL)')
      WHEN 'DESCAMPANIA' THEN COALESCE(t.DESCAMPANIA,'(NULL)')
      WHEN 'BRACKETATENCIONANALISTA' THEN COALESCE(t.BRACKETATENCIONANALISTA,'(NULL)')
      WHEN 'BRACKETDERIVACIONANALISTA' THEN COALESCE(t.BRACKETDERIVACIONANALISTA,'(NULL)')
      WHEN 'NIVELAUTONOMIA' THEN COALESCE(t.NIVELAUTONOMIA,'(NULL)')
      WHEN 'DESTIPRENTA' THEN COALESCE(t.DESTIPRENTA,'(NULL)')
      WHEN 'PRODUCTO' THEN COALESCE(t.PRODUCTO,'(NULL)')
      WHEN 'DESPRODUCTO' THEN COALESCE(t.DESPRODUCTO,'(NULL)')
    END AS dim_value,
    t.FLGAPROBADO
  FROM CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.T72496_HM_SOLICITUDES_SCRM t
  CROSS JOIN dims d
  JOIN params p ON t.CODMES IN (p.p1, p.p2)
),

agg AS (
  SELECT
    CODMES,
    dim_name,
    dim_value,
    COUNT(*) AS n,
    SUM(FLGAPROBADO) AS a
  FROM long_base
  GROUP BY CODMES, dim_name, dim_value
),

rates AS (
  SELECT
    CODMES,
    dim_name,
    dim_value,
    n,
    a,
    CAST(a AS DOUBLE) / NULLIF(n,0) AS r,                                -- tasa en el grupo
    CAST(n AS DOUBLE) / SUM(n) OVER (PARTITION BY CODMES, dim_name) AS w -- peso del grupo
  FROM agg
),

t1 AS (
  SELECT r.* FROM rates r JOIN params p ON r.CODMES = p.p1
),
t2 AS (
  SELECT r.* FROM rates r JOIN params p ON r.CODMES = p.p2
),

decomp AS (
  SELECT
    COALESCE(t1.dim_name, t2.dim_name) AS dim_name,
    COALESCE(t1.dim_value, t2.dim_value) AS dim_value,

    COALESCE(t1.w, 0D) AS w1,
    COALESCE(t2.w, 0D) AS w2,
    COALESCE(t1.r, 0D) AS r1,
    COALESCE(t2.r, 0D) AS r2,

    (COALESCE(t2.w,0D) - COALESCE(t1.w,0D)) * COALESCE(t1.r,0D) AS mix,
    COALESCE(t1.w,0D) * (COALESCE(t2.r,0D) - COALESCE(t1.r,0D)) AS perf,
    (COALESCE(t2.w,0D) - COALESCE(t1.w,0D)) * (COALESCE(t2.r,0D) - COALESCE(t1.r,0D)) AS inter
  FROM t1
  FULL OUTER JOIN t2
    ON t1.dim_name = t2.dim_name
   AND t1.dim_value = t2.dim_value
),

driver_dim AS (
  SELECT
    dim_name,
    -- “Magnitud explicada” por la dimensión (suma de contribuciones absolutas)
    SUM(ABS(mix) + ABS(perf) + ABS(inter)) AS driver_score,
    -- También útil: contribución neta (con signo) para ver dirección total
    SUM(mix + perf + inter) AS net_contribution
  FROM decomp
  GROUP BY dim_name
)

SELECT
  dim_name,
  driver_score,
  net_contribution,
  driver_score * 100 AS driver_score_pp,
  net_contribution * 100 AS net_contribution_pp,
  DENSE_RANK() OVER (ORDER BY driver_score DESC) AS driver_rank
FROM driver_dim
ORDER BY driver_rank;
