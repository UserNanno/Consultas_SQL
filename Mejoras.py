# Limpieza de texto
for col in ['ANALISTA', 'PRODUCTO', 'RESULTADOANALISTA', 'SUBMOTIVOMALADERIVACION']:
    tp_powerapp[col] = tp_powerapp[col].astype(str).str.strip().str.upper()
