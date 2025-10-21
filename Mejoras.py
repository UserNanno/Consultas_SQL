tp_centralizado['FLGQUINCENA'] = (
    pd.to_numeric(tp_centralizado['Dia'], errors='coerce').le(15)
).astype(int)

# 2) EQUIPOBEX: map por Área desde BEX.csv
df_bex = pd.read_csv('INPUT/BEX.csv', encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])
bex_map = df_bex.set_index('AREA')['EQUIPO']
tp_centralizado['EQUIPOBEX'] = tp_centralizado['UnidadOrganizativa_CENTRALIZADO'].map(bex_map)

# 3) EQUIPOENALTA: map por Área desde ENALTA.csv
df_enalta = pd.read_csv('INPUT/ENALTA.csv', encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])
enalta_map = df_enalta.set_index('AREA')['EQUIPO']
tp_centralizado['EQUIPOENALTA'] = tp_centralizado['UnidadOrganizativa_CENTRALIZADO'].map(enalta_map)


---------------------------------------------------------------------------
InvalidIndexError                         Traceback (most recent call last)
Cell In[7], line 14
     12 df_bex = pd.read_csv(RUTA_EQUIPO_BEX, encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])
     13 bex_map = df_bex.set_index('AREA')['EQUIPO']
---> 14 tp_centralizado['EQUIPOBEX'] = tp_centralizado['UnidadOrganizativa_CENTRALIZADO'].map(bex_map)
     16 # 3) EQUIPOENALTA: map por Área desde ENALTA.csv
     17 df_enalta = pd.read_csv(RUTA_EQUIPO_ENALTA, encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\core\series.py:4719, in Series.map(self, arg, na_action)
   4639 def map(
   4640     self,
   4641     arg: Callable | Mapping | Series,
   4642     na_action: Literal["ignore"] | None = None,
   4643 ) -> Series:
   4644     """
   4645     Map values of Series according to an input mapping or function.
   4646 
   (...)   4717     dtype: object
   4718     """
-> 4719     new_values = self._map_values(arg, na_action=na_action)
   4720     return self._constructor(new_values, index=self.index, copy=False).__finalize__(
   4721         self, method="map"
   4722     )

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\core\base.py:925, in IndexOpsMixin._map_values(self, mapper, na_action, convert)
    922 if isinstance(arr, ExtensionArray):
    923     return arr.map(mapper, na_action=na_action)
--> 925 return algorithms.map_array(arr, mapper, na_action=na_action, convert=convert)

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\core\algorithms.py:1732, in map_array(arr, mapper, na_action, convert)
   1728     mapper = mapper[mapper.index.notna()]
   1730 # Since values were input this means we came from either
   1731 # a dict or a series and mapper should be an index
-> 1732 indexer = mapper.index.get_indexer(arr)
   1733 new_values = take_nd(mapper._values, indexer)
   1735 return new_values

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\core\indexes\base.py:3892, in Index.get_indexer(self, target, method, limit, tolerance)
   3889 self._check_indexing_method(method, limit, tolerance)
   3891 if not self._index_as_unique:
-> 3892     raise InvalidIndexError(self._requires_unique_msg)
   3894 if len(target) == 0:
   3895     return np.array([], dtype=np.intp)

InvalidIndexError: Reindexing only valid with uniquely valued Index objects

