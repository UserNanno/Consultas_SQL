# 6) ORDEN / VISTA RÁPIDA
cols_nuevas = [
    "FLGMALDERIVADO","MOTIVO_MALDERIVADO","CODMES","LLAVEMATRICULA",
    "AGENCIA","GERENTE_AGENCIA","UNIDAD_ORGANICA","SERVICIO_TRIBU_COE",
    "TIEMPO_ASESOR","FLG_TIEMPO"
]
# Mover nuevas al final si existen
for c in cols_nuevas:
    if c in tp_centralizado.columns:
        tp_centralizado = tp_centralizado[[col for col in tp_centralizado.columns if col != c] + [c]]

print(tp_centralizado[cols_nuevas].head(10))


---------------------------------------------------------------------------
KeyError                                  Traceback (most recent call last)
Cell In[16], line 12
      9     if c in tp_centralizado.columns:
     10         tp_centralizado = tp_centralizado[[col for col in tp_centralizado.columns if col != c] + [c]]
---> 12 print(tp_centralizado[cols_nuevas].head(10))

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\core\frame.py:4119, in DataFrame.__getitem__(self, key)
   4117     if is_iterator(key):
   4118         key = list(key)
-> 4119     indexer = self.columns._get_indexer_strict(key, "columns")[1]
   4121 # take() does not accept boolean indexers
   4122 if getattr(indexer, "dtype", None) == bool:

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\core\indexes\base.py:6212, in Index._get_indexer_strict(self, key, axis_name)
   6209 else:
   6210     keyarr, indexer, new_indexer = self._reindex_non_unique(keyarr)
-> 6212 self._raise_if_missing(keyarr, indexer, axis_name)
   6214 keyarr = self.take(indexer)
   6215 if isinstance(key, Index):
   6216     # GH 42790 - Preserve name from an Index

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\core\indexes\base.py:6264, in Index._raise_if_missing(self, key, indexer, axis_name)
   6261     raise KeyError(f"None of [{key}] are in the [{axis_name}]")
   6263 not_found = list(ensure_index(key)[missing_mask.nonzero()[0]].unique())
-> 6264 raise KeyError(f"{not_found} not in index")

KeyError: "['AGENCIA', 'UNIDAD_ORGANICA'] not in index"
