# EJECUCIÓN
df_org = load_organico(spark, BASE_DIR_ORGANICO).cache()
df_org.count()   # materializa lectura + normalización

df_diccionario = build_diccionario_actores(
    spark,
    path_estados=PATH_SF_ESTADOS,
    path_productos=PATH_SF_PRODUCTOS,
    filtrar_stopwords=True
).cache()
df_diccionario.count()  # materializa lectura SF + tokens

matches_top = map_diccionario_a_organico(
    df_diccionario=df_diccionario,
    df_org=df_org,
    min_tokens=3,
    usar_stopwords=True,
    alinear_mes=True,
    devolver_todos=False
).cache()
matches_top.count()  # materializa el join pesado de tokens UNA vez

df_pa = load_powerapps(spark, PATH_PA_SOLICITUDES).cache()
df_pa.count()  # materializa lectura/dedup de PA

# Incorporar solo los analistas que aparecen por correo en PowerApps y no existían ya en matches_top
matches_final = incorporar_powerapps_en_matches(
    matches_top=matches_top,
    df_pa=df_pa,
    df_org=df_org,
    alinear_mes=True
)

# (opcional) si vas a usar matches_final varias veces:
# matches_final = matches_final.cache()
# matches_final.count()
