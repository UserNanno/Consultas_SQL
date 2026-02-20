SELECT
  dim_value,
  (mix+perf+inter)*100 AS contrib_pp,
  mix*100 AS mix_pp,
  perf*100 AS perf_pp
FROM decomp
WHERE dim_name = 'DESPRODUCTO'
ORDER BY ABS(mix+perf+inter) DESC;
