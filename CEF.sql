CALENDARIO AS (
    SELECT explode(
        sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)
    ) AS FECHA
),
DIMCALENDARIO AS (
    SELECT
        A.FECHA,
        (CASE
            WHEN dayofweek(A.FECHA) = 1 THEN 0       -- domingo (1) o sábado (7)
            WHEN B.FECHA IS NOT NULL THEN 0              -- feriado
            ELSE 1
        END) AS FLGDIAUTIL
    FROM CALENDARIO A
    LEFT JOIN CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_DE_DIASNOHABILES B ON A.FECHA = B.FECHA
),
