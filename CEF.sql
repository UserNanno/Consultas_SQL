from pyspark.sql import functions as F

GERENTE_MAT = "U17293"

def aplicar_validacion_y_regla_autonomia_monto(df_final):
    """
    Capa final de autonomía por MONTO:
    - Si FLGAUTONOMIA=0 -> asigna autonomía por monto (si aplica)
    - Si FLGAUTONOMIA=1 -> valida contra monto; si no coincide, corrige y marca FLG_CORRECCION_MONTO=1
    Además crea/actualiza:
      - REGLAAUTONOMIA: PASO | MONTO | PASO+MONTO | NINGUNA
      - FLG_CORRECCION_MONTO: 0/1
    Reglas monto:
      - >240000 => GERENTE (U17293)
      - >100000 & <=240000 => SUPERVISOR (MATSUPERIOR)
      - <=100000 => NINGUNA
    """

    # asegurar flags base
    df = (
        df_final
        .withColumn("FLGAUTONOMIA", F.coalesce(F.col("FLGAUTONOMIA"), F.lit(0)))
        .withColumn("FLGAUTONOMIAOBSERVADA", F.coalesce(F.col("FLGAUTONOMIAOBSERVADA"), F.lit(0)))
    )

    # Monto ya viene decimal(18,2) según tu loader, pero lo dejamos “doble seguro”
    mto = F.col("MTOAPROBADO").cast("decimal(18,2)")

    cond_ger = mto > F.lit(240000)
    cond_sup = (mto > F.lit(100000)) & (mto <= F.lit(240000))
    cond_none = (mto.isNull()) | (mto <= F.lit(100000))

    # “esperado por monto”
    nivel_monto = (
        F.when(cond_ger, F.lit("GERENTE"))
         .when(cond_sup, F.lit("SUPERVISOR"))
         .otherwise(F.lit(None))
    )

    mataut_monto = (
        F.when(cond_ger, F.lit(GERENTE_MAT))
         .when(cond_sup, F.col("MATSUPERIOR"))
         .otherwise(F.lit(None))
    )

    flg_monto_aplica = (
        nivel_monto.isNotNull()
        & mataut_monto.isNotNull()
        & mto.isNotNull()
    )

    # REGLAAUTONOMIA base
    df = df.withColumn(
        "REGLAAUTONOMIA",
        F.when(F.col("FLGAUTONOMIA") == 1, F.lit("PASO")).otherwise(F.lit("NINGUNA"))
    )

    # ------------------------------------------------------------------
    # CASO A: FLGAUTONOMIA = 0  => aplicar monto (si corresponde)
    # ------------------------------------------------------------------
    aplica_en_sin_paso = (F.col("FLGAUTONOMIA") == 0)

    df = (
        df
        .withColumn(
            "FLGAUTONOMIA",
            F.when(aplica_en_sin_paso & flg_monto_aplica, F.lit(1)).otherwise(F.col("FLGAUTONOMIA"))
        )
        .withColumn(
            "NIVELAUTONOMIA",
            F.when(aplica_en_sin_paso & flg_monto_aplica, nivel_monto).otherwise(F.col("NIVELAUTONOMIA"))
        )
        .withColumn(
            "MATAUTONOMIA",
            F.when(aplica_en_sin_paso & flg_monto_aplica, mataut_monto).otherwise(F.col("MATAUTONOMIA"))
        )
        .withColumn(
            "PASO_AUTONOMIA",
            F.when(aplica_en_sin_paso & flg_monto_aplica, F.lit("REGLA_MONTO")).otherwise(F.col("PASO_AUTONOMIA"))
        )
        .withColumn(
            "TS_AUTONOMIA",
            F.when(aplica_en_sin_paso & flg_monto_aplica, F.col("TS_PRODUCTOS")).otherwise(F.col("TS_AUTONOMIA"))
        )
        .withColumn(
            "REGLAAUTONOMIA",
            F.when(aplica_en_sin_paso & flg_monto_aplica, F.lit("MONTO")).otherwise(F.col("REGLAAUTONOMIA"))
        )
    )

    # ------------------------------------------------------------------
    # CASO B: FLGAUTONOMIA = 1  => validar vs monto; corregir si no coincide
    # ------------------------------------------------------------------
    tenia_paso = (F.col("REGLAAUTONOMIA") == "PASO")  # después de lo anterior, PASO significa “venía por paso”

    # Determinar si el "PASO" está OK con el monto:
    # - Si monto exige GERENTE => NIVEL debe ser GERENTE y MAT debe ser U17293
    # - Si monto exige SUPERVISOR => NIVEL SUPERVISOR y MAT debe ser MATSUPERIOR
    # - Si monto exige NINGUNA (<=100k o null) => entonces NO debería existir autonomía (FLGAUTONOMIA debería ser 0)
    ok_con_monto = (
        F.when(cond_ger, (F.col("NIVELAUTONOMIA") == "GERENTE") & (F.col("MATAUTONOMIA") == F.lit(GERENTE_MAT)))
         .when(cond_sup, (F.col("NIVELAUTONOMIA") == "SUPERVISOR") & (F.col("MATAUTONOMIA") == F.col("MATSUPERIOR")))
         .when(cond_none, F.lit(False))  # si <=100k, un PASO nunca es “válido”, debe apagarse
         .otherwise(F.lit(False))
    )

    necesita_correccion = tenia_paso & (~ok_con_monto)

    # Corrección: setear exactamente lo que diga el monto, o apagar si no aplica
    df = (
        df
        .withColumn("FLG_CORRECCION_MONTO", F.when(necesita_correccion, F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "FLGAUTONOMIA",
            F.when(necesita_correccion & flg_monto_aplica, F.lit(1))  # sigue existiendo por monto
             .when(necesita_correccion & (~flg_monto_aplica), F.lit(0))  # se apaga (<=100k o datos insuficientes)
             .otherwise(F.col("FLGAUTONOMIA"))
        )
        .withColumn(
            "NIVELAUTONOMIA",
            F.when(necesita_correccion & flg_monto_aplica, nivel_monto)
             .when(necesita_correccion & (~flg_monto_aplica), F.lit(None))
             .otherwise(F.col("NIVELAUTONOMIA"))
        )
        .withColumn(
            "MATAUTONOMIA",
            F.when(necesita_correccion & flg_monto_aplica, mataut_monto)
             .when(necesita_correccion & (~flg_monto_aplica), F.lit(None))
             .otherwise(F.col("MATAUTONOMIA"))
        )
        .withColumn(
            "PASO_AUTONOMIA",
            F.when(necesita_correccion & flg_monto_aplica, F.lit("CORRECCION_MONTO"))
             .when(necesita_correccion & (~flg_monto_aplica), F.lit("ANULADO_POR_MONTO"))
             .otherwise(F.col("PASO_AUTONOMIA"))
        )
        .withColumn(
            "TS_AUTONOMIA",
            F.when(necesita_correccion, F.col("TS_PRODUCTOS")).otherwise(F.col("TS_AUTONOMIA"))
        )
        .withColumn(
            "REGLAAUTONOMIA",
            F.when(necesita_correccion, F.lit("PASO+MONTO")).otherwise(F.col("REGLAAUTONOMIA"))
        )
    )

    return df










# 9) POWERAPPS (fallback SOLO si sigue NULL) + motivos
df_final = apply_powerapps_fallback(df_final, df_apps)

# 9.1) MATSUPERIOR (orgánico)
df_final = add_matsuperior_from_organico(df_final, df_org)

# 9.2) AUTONOMIA POR MONTO (crear si FLGAUTONOMIA=0, validar/corregir si FLGAUTONOMIA=1)
df_final = aplicar_validacion_y_regla_autonomia_monto(df_final)










    # Autonomía
    "FLGAUTONOMIA",
    "FLGAUTONOMIAOBSERVADA",
    "NIVELAUTONOMIA",
    "MATAUTONOMIA",
    "PASO_AUTONOMIA",
    "TS_AUTONOMIA",
    "REGLAAUTONOMIA",
    "FLG_CORRECCION_MONTO",
