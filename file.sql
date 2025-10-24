# Lista de analistas a excluir
analistas_excluir = [
    'ANALISTA1',
    'ANALISTA2',
    'ANALISTA3'
]

# Filtrar: quedarse con todos los que NO estén en esa lista
tp_powerapp = tp_powerapp[~tp_powerapp['ANALISTA'].isin(analistas_excluir)]
