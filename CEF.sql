df_autonomia = (
    df_aut_latest
    .withColumn("ROL_MAT1", rol_actor(F.col("MAT1_AUT")))
    .withColumn("ROL_MAT2", rol_actor(F.col("MAT2_AUT")))
    .withColumn(
        "FLGAUTONOMIAOBSERVADA",
        F.when(
            (F.col("MAT1_AUT").isNotNull()) & (F.col("MAT2_AUT").isNotNull()) &
            (F.col("ROL_MAT1").isin("GERENTE", "SUPERVISOR")) &
            (F.col("ROL_MAT2").isin("GERENTE", "SUPERVISOR")) &
            (F.col("ROL_MAT1") != F.col("ROL_MAT2")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "NIVELAUTONOMIA",
        F.when((F.col("ROL_MAT1") == "GERENTE") & (F.col("ROL_MAT2") == "GERENTE"), F.lit("GERENTE"))
         .when((F.col("ROL_MAT1") == "SUPERVISOR") & (F.col("ROL_MAT2") == "SUPERVISOR"), F.lit("SUPERVISOR"))
         .when(F.col("FLGAUTONOMIAOBSERVADA") == 1, F.col("ROL_MAT1"))
         .when((~es_sup_o_ger(F.col("MAT1_AUT"))) & (~es_sup_o_ger(F.col("MAT2_AUT"))), F.lit("ANALISTA"))
         .otherwise(F.coalesce(F.col("ROL_MAT1"), F.col("ROL_MAT2")))
    )
    .withColumn(
        "MATAUTONOMIA",
        F.when((F.col("ROL_MAT1") == "GERENTE") & (F.col("ROL_MAT2") == "GERENTE"), F.col("MAT1_AUT"))
         .when((F.col("ROL_MAT1") == "SUPERVISOR") & (F.col("ROL_MAT2") == "SUPERVISOR"), F.col("MAT1_AUT"))
         .when(F.col("FLGAUTONOMIAOBSERVADA") == 1, F.col("MAT1_AUT"))
         .when((~es_sup_o_ger(F.col("MAT1_AUT"))) & (F.col("MAT1_AUT").isNotNull()), F.col("MAT1_AUT"))
         .otherwise(F.coalesce(F.col("MAT1_AUT"), F.col("MAT2_AUT")))
    )
    # ✅ FLGAUTONOMIA solo si la autonomía por paso quedó bien formada
    .withColumn(
        "FLGAUTONOMIA",
        F.when(F.col("NIVELAUTONOMIA").isNotNull() & F.col("MATAUTONOMIA").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
    .select(
        "CODSOLICITUD",
        "FLGAUTONOMIA",
        "FLGAUTONOMIAOBSERVADA",
        "NIVELAUTONOMIA",
        "MATAUTONOMIA",
        "PASO_AUTONOMIA",
        "TS_AUTONOMIA",
    )
)








    

GERENTE_MAT = "U17293"

def aplicar_reglas_autonomia_monto_y_validar(df_final):
    df = df_final

    # Blindaje de tipo (tu monto ya viene Numero.Decimal, igual no hace daño)
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

    # Se considera "monto aplicable" solo si puedo resolver nivel + responsable
    aplica_monto_completo = (
        F.col("MTOAPROBADO_DEC").isNotNull()
        & nivel_por_monto.isNotNull()
        & mataut_por_monto.isNotNull()
    )

    # ---- Diagnóstico: autonomía por paso incompleta (tu caso típico)
    autonomia_incompleta = (
        (F.col("FLGAUTONOMIA") == 1) &
        (
            F.col("NIVELAUTONOMIA").isNull() |
            F.col("MATAUTONOMIA").isNull() |
            F.col("PASO_AUTONOMIA").isNull() |
            F.col("TS_AUTONOMIA").isNull()
        )
    )

    # Si está incompleta, la tratamos como que NO existió realmente una autonomía por paso
    # (esto arregla tu caso: FLGAUTONOMIA=1 pero todo null)
    df = (
        df
        .withColumn("FLG_AUTONOMIA_PASO_INCOMPLETA", F.when(autonomia_incompleta, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("FLGAUTONOMIA", F.when(autonomia_incompleta, F.lit(0)).otherwise(F.col("FLGAUTONOMIA")))
    )

    # REGLAAUTONOMIA consistente (si no existe o viene null)
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
        .withColumn("FLG_AUTONOMIA_NO_RESUELTA_POR_MATSUPERIOR_NULL",
                    F.when(cond_sup & F.col("MATSUPERIOR").isNull() & F.col("MTOAPROBADO_DEC").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("FLGAUTONOMIA", F.when(aplica_monto_en_flg0, F.lit(1)).otherwise(F.col("FLGAUTONOMIA")))
        .withColumn("NIVELAUTONOMIA", F.when(aplica_monto_en_flg0, nivel_por_monto).otherwise(F.col("NIVELAUTONOMIA")))
        .withColumn("MATAUTONOMIA", F.when(aplica_monto_en_flg0, mataut_por_monto).otherwise(F.col("MATAUTONOMIA")))
        .withColumn("PASO_AUTONOMIA", F.when(aplica_monto_en_flg0, F.lit("REGLA_MONTO")).otherwise(F.col("PASO_AUTONOMIA")))
        .withColumn("TS_AUTONOMIA", F.when(aplica_monto_en_flg0, F.col("TS_PRODUCTOS")).otherwise(F.col("TS_AUTONOMIA")))
        .withColumn("REGLAAUTONOMIA", F.when(aplica_monto_en_flg0, F.lit("MONTO")).otherwise(F.col("REGLAAUTONOMIA")))
        .withColumn("FLG_AUTONOMIA_CORREGIDA_MONTO", F.when(aplica_monto_en_flg0, F.lit(1)).otherwise(F.col("FLG_AUTONOMIA_CORREGIDA_MONTO")))
    )

    # =========================================================
    # (2) FLGAUTONOMIA = 1  => validar contra monto y corregir si toca
    # =========================================================
    hay_paso = (F.col("FLGAUTONOMIA") == 1)

    # Si el monto exige sup/gerente y está resoluble (completo), entonces:
    # - si lo de paso no coincide, corregimos
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
        .withColumn("TS_AUTONOMIA", F.when(inconsistente_con_monto, F.col("TS_PRODUCTOS")).otherwise(F.col("TS_AUTONOMIA")))
        .withColumn(
            "REGLAAUTONOMIA",
            F.when(inconsistente_con_monto, F.lit("PASO+MONTO")).otherwise(F.col("REGLAAUTONOMIA"))
        )
        .withColumn(
            "FLG_AUTONOMIA_CORREGIDA_MONTO",
            F.when(inconsistente_con_monto, F.lit(1)).otherwise(F.col("FLG_AUTONOMIA_CORREGIDA_MONTO"))
        )
    )

    # Limpieza
    df = df.drop("MTOAPROBADO_DEC")

    return df

