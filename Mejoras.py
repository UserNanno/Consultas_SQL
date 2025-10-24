import unicodedata

# Función para eliminar tildes de texto
def quitar_tildes(s):
    if isinstance(s, str):
        return ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )
    return s

# Aplicar a todo el DataFrame solo en columnas de texto
for col in tp_powerapp.select_dtypes(include=['object']).columns:
    tp_powerapp[col] = tp_powerapp[col].apply(quitar_tildes)


cols_texto = ['ANALISTA', 'PRODUCTO', 'RESULTADOANALISTA', 'SUBMOTIVOMALADERIVACION']
for col in cols_texto:
    tp_powerapp[col] = tp_powerapp[col].apply(quitar_tildes)
