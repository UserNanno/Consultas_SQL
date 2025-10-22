df_filtrado_excluir["CODMES"].value_counts()

CODMES
202501    9922
202502    9341
202503    8260
202509    8259
202505    7543
202504    7446
202506    7395
202508    6702
202507    6618
202510    4249
Name: count, dtype: int64


df_filtrado_excluir.groupby("CODMES")["Submotivo_MD"].apply(lambda x: x.isna().sum())

CODMES
202501    9922
202502    9340
202503    8260
202504    7445
202505    7537
202506       0
202507    6617
202508    2579
202509    3607
202510    1788
Name: Submotivo_MD, dtype: int64
