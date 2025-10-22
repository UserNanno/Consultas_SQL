df_filtrado_excluir[
    (df_filtrado_excluir["CODMES"] == 202509) &
    (df_filtrado_excluir["Submotivo_MD"].notna())
]
