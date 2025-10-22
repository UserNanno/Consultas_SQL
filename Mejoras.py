df_derivadas["FechaAsignacion"] = pd.to_datetime(df_derivadas["FechaAsignacion"], errors="coerce")
C:\Users\T72496\AppData\Local\Temp\ipykernel_6308\1183093861.py:1: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df_derivadas["FechaAsignacion"] = pd.to_datetime(df_derivadas["FechaAsignacion"], errors="coerce")
