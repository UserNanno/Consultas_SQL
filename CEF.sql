# EJECUCIÓN
df_org = load_organico(spark, BASE_DIR_ORGANICO)

df_diccionario = build_diccionario_actores(
    spark,
    path_estados=PATH_SF_ESTADOS,
    path_productos=PATH_SF_PRODUCTOS,
    filtrar_stopwords=True
)

matches_top = map_diccionario_a_organico(
    df_diccionario=df_diccionario,
    df_org=df_org,
    min_tokens=3,
    usar_stopwords=True,
    alinear_mes=True,
    devolver_todos=False
)

df_pa = load_powerapps(spark, PATH_PA_SOLICITUDES)

# Incorporar solo los analistas que aparecen por correo en PowerApps y no existían ya en matches_top
matches_final = incorporar_powerapps_en_matches(
    matches_top=matches_top,
    df_pa=df_pa,
    df_org=df_org,
    alinear_mes=True
)
