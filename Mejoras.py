Totales por CODMES (antes de filtrar):
CODMES
202501    22972
202502    21448
202503    20574
202504    19678
202505    19579
202506    19193
202507    17866
202508    17808
202509    20931
202510    10544
Name: count, dtype: int64

Filas que pasan el filtro (SIN dedupe) por CODMES:
CODMES
202501    9922
202502    9336
202503    8264
202504    7445
202505    7537
202507    6617
202508    4224
202509    4736
202510    2497
Name: count, dtype: int64

--- Debug junio 202506 ---
Total junio: 19193
No excluidos (TIPOPRODUCTO): 14899
Denegado analista (match exacto): 8145
MOTIVO != 'NAN': 2471
Intersección (tres condiciones): 0

Combinaciones de condiciones en junio:
 m_tipo  m_ra  m_motivo  conteo
   True False     False    7495
   True  True     False    7403
  False False     False    1812
  False False      True    1740
  False  True      True     730
  False  True     False      12
   True False      True       1

Conteo por mes SIN dedupe:
CODMES
202501    9922
202502    9336
202503    8264
202504    7445
202505    7537
202507    6617
202508    4224
202509    4736
202510    2497
Name: count, dtype: int64

Conteo por mes CON dedupe (keep='first'):
CODMES
202501    9901
202502    9318
202503    8252
202504    7439
202505    7524
202507    6607
202508    4220
202509    4728
202510    2495
Name: count, dtype: int64

Junio 202506 - antes dedupe: 0 | después dedupe: 0

Oportunidades de junio que también aparecen en otros meses (antes del dedupe): 0

Mes 'ganador' por OPORTUNIDAD (el que queda tras keep='first'):
CODMES
202501    9901
202502    9318
202503    8252
202504    7439
202505    7524
202507    6607
202508    4220
202509    4728
202510    2495
Name: count, dtype: int64

Conteo por mes CON dedupe (keep='last'):
CODMES
202501    9901
202502    9316
202503    8253
202504    7440
202505    7524
202507    6607
202508    4220
202509    4728
202510    2495
Name: count, dtype: int64
