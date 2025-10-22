valores_incluir = ["Activo", "Pendiente"]
df_filtrado_incluir = df_unido[df_unido["Estado"].isin(valores_incluir)]


valores_excluir = ["Inactivo", "Cancelado"]
df_filtrado_excluir = df_unido[~df_unido["Estado"].isin(valores_excluir)]
