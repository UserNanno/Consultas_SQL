df_org = load_organico(...).cache()
_ = df_org.count()

df_diccionario = build_diccionario_actores(...).cache()
_ = df_diccionario.count()

matches_top = map_diccionario_a_organico(...).cache()
_ = matches_top.count()
