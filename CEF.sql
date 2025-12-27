from pyspark.sql import functions as F

GERENTE_MAT = "U17293"

def aplicar_reglas_autonomia_monto_y_validar(df_final):
    df = df_final

    # ---------------------------------------------------------
    # 0) Blindaje de columnas que pueden NO existir en el df
    # ---------------------------------------------------------
    if "REGLAAUTONOMIA" not in df.columns:
        df = df.withColumn("REGLAAUTONOMIA", F.lit(None).cast("string"))

    # Si TS_PRODUCTOS no existe, define un timestamp fallback para "regla monto"
    # (ajusta el orden de preferencia a tu gusto)
    if "TS_PRODUCTOS" not in df.columns:
        df = df.withColumn("TS_PRODUCTOS", F.lit(None).cast("timestamp"))

    ts_monto = F.coalesce(
        F.col("TS_PRODUCTOS"),
        F.col("TS_BASE_ESTADOS") if "TS_BASE_ESTADOS" in df.columns else F.lit(None).cast("timestamp"),
        F.col("TS_AUTONOMIA") if "TS_AUTONOMIA" in df.columns else F.lit(None).cast("timestamp"),
        F.current_timestamp()
    )

    # Blindaje de tipo
    df = df.withColumn("MTOAPROBADO_DEC", F.col("MTOAPROBADO").cast("decimal(18,2)"))

    # Umbrales acordados
    cond_ger = F.col("MTOAPROBADO_DEC") > F.lit(240000)
    cond_sup = (F.col("MTOAPROBADO_DEC") > F.lit(100000)) & (F.col("MTOAPROBADO_DEC") <= F.lit(240000))

    # Target por monto
    nivel_por_monto = (
        F.when(cond_ger, F.lit("GERENTE"))
         .when(cond_sup, F.lit("SUPERVISOR"))
         .otherwise(F.lit(None))
    )

    mataut_por_monto = (
        F.when(cond_ger, F.lit(GERENTE_MAT))
         .when(cond_sup, F.col("MATSUPERIOR"))   # si es null, NO se puede asignar supervisor
         .otherwise(F.lit(None))
    )

    aplica_monto_completo = (
        F.col("MTOAPROBADO_DEC").isNotNull()
        & nivel_por_monto.isNotNull()
        & mataut_por_monto.isNotNull()
    )

    # ---- Diagnóstico: autonomía por paso incompleta
    autonomia_incompleta = (
        (F.col("FLGAUTONOMIA") == 1) &
        (
            F.col("NIVELAUTONOMIA").isNull() |
            F.col("MATAUTONOMIA").isNull() |
            F.col("PASO_AUTONOMIA").isNull() |
            F.col("TS_AUTONOMIA").isNull()
        )
    )

    df = (
        df
        .withColumn("FLG_AUTONOMIA_PASO_INCOMPLETA", F.when(autonomia_incompleta, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("FLGAUTONOMIA", F.when(autonomia_incompleta, F.lit(0)).otherwise(F.col("FLGAUTONOMIA")))
    )

    # REGLAAUTONOMIA consistente
    df = df.withColumn(
        "REGLAAUTONOMIA",
        F.when(F.col("REGLAAUTONOMIA").isNotNull(), F.col("REGLAAUTONOMIA"))
         .when(F.col("FLGAUTONOMIA") == 1, F.lit("PASO"))
         .otherwise(F.lit("NINGUNA"))
    )

    # =========================================================
    # (1) FLGAUTONOMIA = 0  => aplicar monto si corresponde
    # =========================================================
    aplica_monto_en_flg0 = (F.col("FLGAUTONOMIA") == 0) & aplica_monto_completo

    df = (
        df
        .withColumn("FLG_AUTONOMIA_CORREGIDA_MONTO", F.lit(0))
        .withColumn("FLG_AUTONOMIA_VALIDADA_MONTO", F.lit(0))
        .withColumn(
            "FLG_AUTONOMIA_NO_RESUELTA_POR_MATSUPERIOR_NULL",
            F.when(cond_sup & F.col("MATSUPERIOR").isNull() & F.col("MTOAPROBADO_DEC").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("FLGAUTONOMIA", F.when(aplica_monto_en_flg0, F.lit(1)).otherwise(F.col("FLGAUTONOMIA")))
        .withColumn("NIVELAUTONOMIA", F.when(aplica_monto_en_flg0, nivel_por_monto).otherwise(F.col("NIVELAUTONOMIA")))
        .withColumn("MATAUTONOMIA", F.when(aplica_monto_en_flg0, mataut_por_monto).otherwise(F.col("MATAUTONOMIA")))
        .withColumn("PASO_AUTONOMIA", F.when(aplica_monto_en_flg0, F.lit("REGLA_MONTO")).otherwise(F.col("PASO_AUTONOMIA")))
        .withColumn("TS_AUTONOMIA", F.when(aplica_monto_en_flg0, ts_monto).otherwise(F.col("TS_AUTONOMIA")))
        .withColumn("REGLAAUTONOMIA", F.when(aplica_monto_en_flg0, F.lit("MONTO")).otherwise(F.col("REGLAAUTONOMIA")))
        .withColumn("FLG_AUTONOMIA_CORREGIDA_MONTO", F.when(aplica_monto_en_flg0, F.lit(1)).otherwise(F.col("FLG_AUTONOMIA_CORREGIDA_MONTO")))
    )

    # =========================================================
    # (2) FLGAUTONOMIA = 1  => validar contra monto y corregir si toca
    # =========================================================
    hay_paso = (F.col("FLGAUTONOMIA") == 1)

    inconsistente_con_monto = (
        hay_paso
        & aplica_monto_completo
        & (
            (F.col("NIVELAUTONOMIA") != nivel_por_monto) |
            (F.col("MATAUTONOMIA") != mataut_por_monto)
        )
    )

    df = (
        df
        .withColumn("FLG_AUTONOMIA_VALIDADA_MONTO", F.when(hay_paso, F.lit(1)).otherwise(F.col("FLG_AUTONOMIA_VALIDADA_MONTO")))
        .withColumn("NIVELAUTONOMIA", F.when(inconsistente_con_monto, nivel_por_monto).otherwise(F.col("NIVELAUTONOMIA")))
        .withColumn("MATAUTONOMIA", F.when(inconsistente_con_monto, mataut_por_monto).otherwise(F.col("MATAUTONOMIA")))
        .withColumn("PASO_AUTONOMIA", F.when(inconsistente_con_monto, F.lit("REGLA_MONTO")).otherwise(F.col("PASO_AUTONOMIA")))
        .withColumn("TS_AUTONOMIA", F.when(inconsistente_con_monto, ts_monto).otherwise(F.col("TS_AUTONOMIA")))
        .withColumn("REGLAAUTONOMIA", F.when(inconsistente_con_monto, F.lit("PASO+MONTO")).otherwise(F.col("REGLAAUTONOMIA")))
        .withColumn("FLG_AUTONOMIA_CORREGIDA_MONTO", F.when(inconsistente_con_monto, F.lit(1)).otherwise(F.col("FLG_AUTONOMIA_CORREGIDA_MONTO")))
    )

    return df.drop("MTOAPROBADO_DEC")
