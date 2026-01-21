WITH solicitudes_consolidado AS (
   SELECT ...
   FROM md_evaluacionsolicitudcredito
   WHERE ...
)
SELECT *
FROM solicitudes_consolidado
QUALIFY ROW_NUMBER() OVER(
   PARTITION BY codinternocomputacional, codevaluacion
   ORDER BY fecevaluacion DESC
) = 1;
