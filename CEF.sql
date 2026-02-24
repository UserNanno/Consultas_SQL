from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# INCORPORAR POWERAPPS (POR CORREO) A MATCHES - OPTIMIZADO
def incorporar_powerapps_en_matches(matches_top, df_pa, df_org, alinear_mes=True):
    """
    - Correos (CODMES, CORREO_CLEAN) de PowerApps
    - Solo los que NO existen ya en matches_top (correo+mes)
    - Busca en Orgánico (correo+mes) y obtiene NOMBRECOMPLETO, MATORGANICO, MATSUPERIOR, NBRCORTO y CORREO
    - Incorpora a matches_top con unionByName, SIN trazabilidad
    """

    if not alinear_mes:
        # Si en algún momento lo necesitas, se puede extender, pero tu caso es alinear_mes=True
        raise ValueError("Esta versión optimizada está pensada para alinear_mes=True.")

    # 1) Set CHICO de llaves existentes (solo 2 columnas) + broadcast
    existentes = broadcast(
        matches_top
        .select(
            F.col("CODMES").cast("string").alias("CODMES"),
            norm_email(F.col("CORREO")).alias("CORREO_CLEAN")
        )
        .where(
            F.col("CODMES").isNotNull() & (F.col("CODMES") != "") &
            F.col("CORREO_CLEAN").isNotNull() & (F.col("CORREO_CLEAN") != "")
        )
        .dropDuplicates(["CODMES", "CORREO_CLEAN"])
    )

    # 2) Llaves PowerApps (ya deberías traer CORREO_CLEAN desde load_powerapps)
    pa_keys = df_pa.select("CODMES", "CORREO_CLEAN").dropDuplicates(["CODMES", "CORREO_CLEAN"])

    # 3) Anti-join liviano (PA nuevos)
    pa_nuevos = pa_keys.join(existentes, on=["CODMES", "CORREO_CLEAN"], how="left_anti")

    # 4) Base orgánica mínima + broadcast (join exacto por llave)
    org_base = broadcast(
        df_org.select(
            F.col("CODMES").cast("string").alias("CODMES"),
            F.col("CORREO_CLEAN"),
            F.col("NOMBRECOMPLETO"),
            F.col("MATORGANICO"),
            F.col("MATSUPERIOR"),
            F.col("NBRCORTO"),
            F.col("CORREO").alias("CORREO_ORG")
        )
        .where(
            F.col("CODMES").isNotNull() & (F.col("CODMES") != "") &
            F.col("CORREO_CLEAN").isNotNull() & (F.col("CORREO_CLEAN") != "")
        )
        .dropDuplicates(["CODMES", "CORREO_CLEAN", "MATORGANICO"])
    )

    pa_enriq = (
        pa_nuevos.join(org_base, on=["CODMES", "CORREO_CLEAN"], how="inner")
    )

    # 5) Convertir a esquema de matches_top (rellenar métricas con null)
    pa_rows = (
        pa_enriq.select(
            F.col("NOMBRECOMPLETO").alias("NOMBRE"),
            F.col("CODMES").alias("CODMES"),
            F.col("MATORGANICO").alias("MATORGANICO"),
            F.col("MATSUPERIOR").alias("MATSUPERIOR"),
            F.col("CORREO_ORG").alias("CORREO"),
            F.col("NBRCORTO").alias("NBRCORTO"),
            F.lit(None).cast("int").alias("TOKENS_MATCH"),
            F.lit(None).cast("int").alias("D_SIZE"),
            F.lit(None).cast("int").alias("O_SIZE"),
            F.lit(None).cast("int").alias("UNION_SIZE"),
            F.lit(None).cast("double").alias("JACCARD"),
        )
    )

    return matches_top.unionByName(pa_rows)
