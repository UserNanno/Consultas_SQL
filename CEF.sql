Aqui no te estas olvidando que ya tengo esto?

    # =========================================================
# PARAMETROS AUTONOMIA
# =========================================================
gerentes = ["U17293"]
supervisores = ["U17560", "U13421", "S18795", "U18900", "E12624", "U23436"]

def rol_actor(col_mat):
    return (
        F.when(col_mat.isin(gerentes), F.lit("GERENTE"))
         .when(col_mat.isin(supervisores), F.lit("SUPERVISOR"))
         .when(col_mat.isNotNull(), F.lit("ANALISTA"))
         .otherwise(F.lit(None))
    )

def es_sup_o_ger(col_mat):
    return col_mat.isin(gerentes + supervisores)

from pyspark.sql import functions as F

GERENTE_MAT = "U17293"

def aplicar_regla_autonomia_monto(df_final):
    # asegurar tipo decimal aunque venga bien (doble seguridad)
    df = df_final.withColumn("MTOAPROBADO_DEC", F.col("MTOAPROBADO").cast("decimal(18,2)"))

    # flags de umbral
    cond_ger = F.col("MTOAPROBADO_DEC") > F.lit(240000)
    cond_sup = (F.col("MTOAPROBADO_DEC") > F.lit(100000)) & (F.col("MTOAPROBADO_DEC") <= F.lit(240000))

    # regla monto SOLO si NO hubo regla de paso
    aplica_monto = (F.col("FLGAUTONOMIA") == F.lit(0)) & F.col("MTOAPROBADO_DEC").isNotNull()

    # definir valores por monto
    nivel_monto = (
        F.when(aplica_monto & cond_ger, F.lit("GERENTE"))
         .when(aplica_monto & cond_sup, F.lit("SUPERVISOR"))
         .otherwise(F.lit(None))
    )

    mataut_monto = (
        F.when(aplica_monto & cond_ger, F.lit(GERENTE_MAT))
         .when(aplica_monto & cond_sup, F.col("MATSUPERIOR"))
         .otherwise(F.lit(None))
    )

    flg_monto = F.when(nivel_monto.isNotNull() & mataut_monto.isNotNull(), F.lit(1)).otherwise(F.lit(0))

    # REGLAAUTONOMIA base (si no existe aún)
    df = df.withColumn(
        "REGLAAUTONOMIA",
        F.when(F.col("FLGAUTONOMIA") == 1, F.lit("PASO")).otherwise(F.lit("NINGUNA"))
    )

    # aplicar overwrite SOLO si regla monto determinó autonomía
    df = (
        df
        .withColumn("FLGAUTONOMIA", F.when(flg_monto == 1, F.lit(1)).otherwise(F.col("FLGAUTONOMIA")))
        .withColumn("NIVELAUTONOMIA", F.when(flg_monto == 1, nivel_monto).otherwise(F.col("NIVELAUTONOMIA")))
        .withColumn("MATAUTONOMIA", F.when(flg_monto == 1, mataut_monto).otherwise(F.col("MATAUTONOMIA")))
        .withColumn("PASO_AUTONOMIA", F.when(flg_monto == 1, F.lit("REGLA_MONTO")).otherwise(F.col("PASO_AUTONOMIA")))
        .withColumn("TS_AUTONOMIA", F.when(flg_monto == 1, F.col("TS_PRODUCTOS")).otherwise(F.col("TS_AUTONOMIA")))
        .withColumn(
            "REGLAAUTONOMIA",
            F.when(flg_monto == 1,
                   F.when(F.col("REGLAAUTONOMIA") == "PASO", F.lit("PASO+MONTO")).otherwise(F.lit("MONTO"))
            ).otherwise(F.col("REGLAAUTONOMIA"))
        )
        .drop("MTOAPROBADO_DEC")
    )

    return df









df_final = apply_powerapps_fallback(df_final, df_apps)
df_final = add_matsuperior_from_organico(df_final, df_org)

# 9.6) AUTONOMIA POR MONTO (solo si no hubo PASO) + REGLAAUTONOMIA
df_final = aplicar_regla_autonomia_monto(df_final)

# 10) SELECT FINAL...

