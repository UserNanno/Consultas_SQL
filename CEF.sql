DIMCALENDARIO AS (
  SELECT
    A.FECHA,
    date_format(A.FECHA,'yyyyMM') AS CODMES,

    -- Peso del día útil equivalente
    CASE
      WHEN B.FECHA IS NOT NULL THEN 0.0             -- feriado
      WHEN dayofweek(A.FECHA) = 1 THEN 0.0          -- domingo
      WHEN dayofweek(A.FECHA) = 7 THEN 0.5          -- sábado
      ELSE 1.0                                      -- lun-vie
    END AS PESO_DIA_UTIL_EQ,

    -- Flag simple (si quieres mantenerlo)
    CASE
      WHEN B.FECHA IS NOT NULL THEN 0
      WHEN dayofweek(A.FECHA) = 1 THEN 0
      ELSE 1
    END AS FLGDIAUTIL,

    -- Acumulado de días útiles equivalentes dentro del mes (1,2,2.5,3.5,...)
    SUM(
      CASE
        WHEN B.FECHA IS NOT NULL THEN 0.0
        WHEN dayofweek(A.FECHA) = 1 THEN 0.0
        WHEN dayofweek(A.FECHA) = 7 THEN 0.5
        ELSE 1.0
      END
    ) OVER (
      PARTITION BY date_format(A.FECHA,'yyyyMM')
      ORDER BY A.FECHA
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ACUM_DIA_UTIL_EQ_MES

  FROM CALENDARIO A
  LEFT JOIN (
    SELECT DISTINCT FECHA
    FROM CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_DE_DIASNOHABILES
  ) B ON A.FECHA = B.FECHA
)







LEFT JOIN DIMCALENDARIO C ON (A.FECSOLICITUD = C.FECHA)
...
(CASE WHEN C.FLGDIAUTIL = 1 THEN 'SI' ELSE 'NO' END) AS FLGDIAUTIL,




C.PESO_DIA_UTIL_EQ,
C.ACUM_DIA_UTIL_EQ_MES,
