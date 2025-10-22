df_filtrado_excluir.groupby("codmes")["Submotivo_MD"].apply(lambda x: x.isna().sum())
