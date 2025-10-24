df_analistas = pd.read_csv('INPUT/POWERAPP/ANALISTAS.csv', sep = ';')


---------------------------------------------------------------------------
UnicodeDecodeError                        Traceback (most recent call last)
Cell In[77], line 1
----> 1 df_analistas = pd.read_csv('INPUT/POWERAPP/ANALISTAS.csv', sep = ';')

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\io\parsers\readers.py:1026, in read_csv(filepath_or_buffer, sep, delimiter, header, names, index_col, usecols, dtype, engine, converters, true_values, false_values, skipinitialspace, skiprows, skipfooter, nrows, na_values, keep_default_na, na_filter, verbose, skip_blank_lines, parse_dates, infer_datetime_format, keep_date_col, date_parser, date_format, dayfirst, cache_dates, iterator, chunksize, compression, thousands, decimal, lineterminator, quotechar, quoting, doublequote, escapechar, comment, encoding, encoding_errors, dialect, on_bad_lines, delim_whitespace, low_memory, memory_map, float_precision, storage_options, dtype_backend)
   1013 kwds_defaults = _refine_defaults_read(
   1014     dialect,
   1015     delimiter,
   (...)   1022     dtype_backend=dtype_backend,
   1023 )
   1024 kwds.update(kwds_defaults)
-> 1026 return _read(filepath_or_buffer, kwds)

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\io\parsers\readers.py:620, in _read(filepath_or_buffer, kwds)
    617 _validate_names(kwds.get("names", None))
    619 # Create the parser.
--> 620 parser = TextFileReader(filepath_or_buffer, **kwds)
    622 if chunksize or iterator:
    623     return parser

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\io\parsers\readers.py:1620, in TextFileReader.__init__(self, f, engine, **kwds)
   1617     self.options["has_index_names"] = kwds["has_index_names"]
   1619 self.handles: IOHandles | None = None
-> 1620 self._engine = self._make_engine(f, self.engine)

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\io\parsers\readers.py:1898, in TextFileReader._make_engine(self, f, engine)
   1895     raise ValueError(msg)
   1897 try:
-> 1898     return mapping[engine](f, **self.options)
   1899 except Exception:
   1900     if self.handles is not None:

File D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pandas\io\parsers\c_parser_wrapper.py:93, in CParserWrapper.__init__(self, src, **kwds)
     90 if kwds["dtype_backend"] == "pyarrow":
     91     # Fail here loudly instead of in cython after reading
     92     import_optional_dependency("pyarrow")
---> 93 self._reader = parsers.TextReader(src, **kwds)
     95 self.unnamed_cols = self._reader.unnamed_cols
     97 # error: Cannot determine type of 'names'

File pandas/_libs/parsers.pyx:574, in pandas._libs.parsers.TextReader.__cinit__()

File pandas/_libs/parsers.pyx:663, in pandas._libs.parsers.TextReader._get_header()

File pandas/_libs/parsers.pyx:874, in pandas._libs.parsers.TextReader._tokenize_rows()

File pandas/_libs/parsers.pyx:891, in pandas._libs.parsers.TextReader._check_tokenize_status()

File pandas/_libs/parsers.pyx:2053, in pandas._libs.parsers.raise_parser_error()

File <frozen codecs>:325, in BufferedIncrementalDecoder.decode(self, input, final)
    322 def decode(self, input, final=False):
    323     # decode input (taking the buffer into account)
    324     data = self.buffer + input
--> 325     (result, consumed) = self._buffer_decode(data, self.errors, final)
    326     # keep undecoded input until the next call
    327     self.buffer = data[consumed:]

UnicodeDecodeError: 'utf-8' codec can't decode byte 0xd1 in position 5440: invalid continuation byte
