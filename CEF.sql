from pyspark.sql import functions as F

def incorporar_powerapps_en_matches(matches_top, df_pa, df_org, alinear_mes=True):
    """
    - Correos (CODMES, CORREO_CLEAN) de PowerApps
    - Solo los que NO existen ya en matches_top (correo+mes)
    - Busca en Orgánico (correo+mes) y obtiene NOMBRECOMPLETO, MATORGANICO, MATSUPERIOR, NBRCORTO y CORREO
    - Incorpora a matches_top con unionByName, SIN trazabilidad
    """

    # 1) Normaliza correo en matches_top
    mt = matches_top.withColumn("CORREO_CLEAN", norm_email(F.col("CORREO")))

    # 2) Anti-join: correos PA que no están ya en matches_top
    if alinear_mes:
        pa_nuevos = df_pa.join(
            mt.select("CODMES", "CORREO_CLEAN").dropDuplicates(),
            on=["CODMES", "CORREO_CLEAN"],
            how="left_anti"
        )
    else:
        pa_nuevos = df_pa.join(
            mt.select("CORREO_CLEAN").dropDuplicates(),
            on=["CORREO_CLEAN"],
            how="left_anti"
        )

    # 3) Base orgánica para enriquecer (OJO: alias CORREO para no ambigüedad)
    org_base = (
        df_org
        .select(
            F.col("CODMES"),
            F.col("CORREO_CLEAN"),
            F.col("NOMBRECOMPLETO"),
            F.col("MATORGANICO"),
            F.col("MATSUPERIOR"),
            F.col("NBRCORTO"),
            F.col("CORREO").alias("CORREO_ORG")   # <- clave para evitar ambigüedad
        )
        .dropDuplicates(["CODMES", "CORREO_CLEAN", "MATORGANICO"])
    )

    join_keys = ["CODMES", "CORREO_CLEAN"] if alinear_mes else ["CORREO_CLEAN"]

    pa_enriq = (
        pa_nuevos.alias("pa")
        .join(org_base.alias("org"), on=join_keys, how="left")
        .where(F.col("org.MATORGANICO").isNotNull())
    )

    # 4) Construir filas con el MISMO esquema que matches_top
    #    (rellenando métricas con null)
    pa_rows = (
        pa_enriq
        .select(
            F.col("org.NOMBRECOMPLETO").alias("NOMBRE"),
            F.col("org.CODMES").alias("CODMES"),
            F.col("org.MATORGANICO").alias("MATORGANICO"),
            F.col("org.MATSUPERIOR").alias("MATSUPERIOR"),
            F.col("org.CORREO_ORG").alias("CORREO"),
            F.col("org.NBRCORTO").alias("NBRCORTO"),
            F.lit(None).cast("int").alias("TOKENS_MATCH"),
            F.lit(None).cast("int").alias("D_SIZE"),
            F.lit(None).cast("int").alias("O_SIZE"),
            F.lit(None).cast("int").alias("UNION_SIZE"),
            F.lit(None).cast("double").alias("JACCARD"),
        )
    )

    # 5) Union final
    return matches_top.unionByName(pa_rows)
