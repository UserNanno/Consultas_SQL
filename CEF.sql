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








# =========================================================
# PIPELINE
# =========================================================

# 1) STAGING
df_org = load_organico(spark, BASE_DIR_ORGANICO)
df_org_tokens = build_org_tokens(df_org)

df_estados = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos = load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps = load_powerapps(spark, PATH_PA_SOLICITUDES)

# 2) ENRIQUECIMIENTO CON ORGANICO
df_estados_enriq = enrich_estados_con_organico(df_estados, df_org_tokens)
df_productos_enriq = enrich_productos_con_organico(df_productos, df_org_tokens)

# 3) REGLAS: MATANALISTA (prioriza ESTADOS, fallback PRODUCTOS solo si ESTADOS no pudo)
is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
is_cef = F.col("PROCESO").isin(
    "CO SOLICITUD APROBACIONES TLMK",
    "SFCP APROBACIONES EDUCATIVO",
    "CO SOLICITUD APROBACIONES"
)
paso_tc_analista   = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
paso_tc_supervisor = (F.col("NBRPASO") == "APROBACION DE CREDITOS SUPERVISOR")
paso_tc_gerente    = (F.col("NBRPASO") == "APROBACION DE CREDITOS GERENTE")
paso_cef_analista  = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")
paso_cef_aprobador = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD APROBADOR")

# Paso base según tipo
es_paso_base = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)
w_base = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORINICIOEVALUACION").desc())

df_base_latest = (
   df_estados_enriq
     .filter(es_paso_base)
     .withColumn("rn_base", F.row_number().over(w_base))
     .filter(F.col("rn_base") == 1)
     .select(
         "CODSOLICITUD",
         F.col("MATORGANICO").alias("MAT1_ESTADOS"),        # desde NBRULTACTOR enriquecido
         F.col("MATORGANICOPASO").alias("MAT2_ESTADOS"),    # desde NBRULTACTORPASO enriquecido
         F.col("FECHORINICIOEVALUACION").alias("TS_BASE_ESTADOS")
     )
)

df_matanalista_estados = (
   df_base_latest
     .withColumn(
         "MATANALISTA_ESTADOS",
         F.when(F.col("MAT1_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT1_ESTADOS"))), F.col("MAT1_ESTADOS"))
          .when(F.col("MAT2_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT2_ESTADOS"))), F.col("MAT2_ESTADOS"))
          .otherwise(F.lit(None).cast("string"))
     )
     .withColumn(
         "ORIGEN_MATANALISTA_ESTADOS",
         F.when(F.col("MAT1_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT1_ESTADOS"))), F.lit("ESTADOS_MAT1"))
          .when(F.col("MAT2_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT2_ESTADOS"))), F.lit("ESTADOS_MAT2"))
          .otherwise(F.lit(None))
     )
     .select("CODSOLICITUD", "MATANALISTA_ESTADOS", "ORIGEN_MATANALISTA_ESTADOS", "TS_BASE_ESTADOS")
)

# Snapshot productos 1 fila por CODSOLICITUD (para MAT3/MAT4)
w_prod_mat = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECCREACION").desc_nulls_last())

df_matanalista_productos = (
    df_productos_enriq
      .withColumn("rn", F.row_number().over(w_prod_mat))
      .filter(F.col("rn") == 1)
      .select(
          "CODSOLICITUD",
          F.col("MATORGANICO_ANALISTA").alias("MAT3_PRODUCTOS"),
          F.col("MATORGANICO_ASIGNADO").alias("MAT4_PRODUCTOS"),
      )
      .drop("rn")
)

df_matanalista_final = (
    df_matanalista_estados
      .join(df_matanalista_productos, on="CODSOLICITUD", how="left")
      # Limpia MAT3/MAT4 si son sup/ger
      .withColumn("MAT3_OK", F.when(~es_sup_o_ger(F.col("MAT3_PRODUCTOS")), F.col("MAT3_PRODUCTOS")))
      .withColumn("MAT4_OK", F.when(~es_sup_o_ger(F.col("MAT4_PRODUCTOS")), F.col("MAT4_PRODUCTOS")))
      .withColumn(
          "MATANALISTA_FINAL",
          F.coalesce(
              F.col("MATANALISTA_ESTADOS"),   # MAT1/MAT2 ya filtrado por tu lógica
              F.col("MAT3_OK"),               # MAT3
              F.col("MAT4_OK")                # MAT4
          )
      )
      .withColumn(
          "ORIGEN_MATANALISTA",
          F.when(F.col("MATANALISTA_ESTADOS").isNotNull(), F.col("ORIGEN_MATANALISTA_ESTADOS"))
           .when(F.col("MAT3_OK").isNotNull(), F.lit("PRODUCTOS_MAT3"))
           .when(F.col("MAT4_OK").isNotNull(), F.lit("PRODUCTOS_MAT4"))
           .otherwise(F.lit(None))
      )
      .drop(
          "MATANALISTA_ESTADOS",
          "ORIGEN_MATANALISTA_ESTADOS",
          "MAT3_PRODUCTOS", "MAT4_PRODUCTOS",
          "MAT3_OK", "MAT4_OK"
      )
)

# 4) REGLAS: AUTONOMIA (desde ESTADOS)
es_paso_autonomia = (is_cef & paso_cef_aprobador) | (is_tc & (paso_tc_supervisor | paso_tc_gerente))
w_aut = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORINICIOEVALUACION").desc())

df_aut_latest = (
    df_estados_enriq
    .filter(es_paso_autonomia)
    .withColumn("rn_aut", F.row_number().over(w_aut))
    .filter(F.col("rn_aut") == 1)
    .select(
        "CODSOLICITUD",
        F.col("MATORGANICO").alias("MAT1_AUT"),
        F.col("MATORGANICOPASO").alias("MAT2_AUT"),
        F.col("NBRPASO").alias("PASO_AUTONOMIA"),
        F.col("FECHORINICIOEVALUACION").alias("TS_AUTONOMIA"),
    )
)

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


# 5) UNIVERSO (ANCLADO A ESTADOS)
df_universo = df_estados_enriq.select("CODSOLICITUD").distinct()

df_final_autonomias = (
    df_universo
    .join(df_matanalista_final, on="CODSOLICITUD", how="left")
    .join(df_autonomia, on="CODSOLICITUD", how="left")
    .withColumn("FLGAUTONOMIA", F.coalesce(F.col("FLGAUTONOMIA"), F.lit(0)))
    .withColumn("FLGAUTONOMIAOBSERVADA", F.coalesce(F.col("FLGAUTONOMIAOBSERVADA"), F.lit(0)))
)
# df_final_autonomias: 1 fila por CODSOLICITUD (solo las que existen en ESTADOS)

# 6) SNAPSHOTS DE ESTADOS + PRODUCTOS
df_last_estado = build_last_estado_snapshot(df_estados_enriq)    # base final
df_prod_snap   = build_productos_snapshot(df_productos_enriq)    # solo atributos

# 7) ENSAMBLE FINAL (base = df_last_estado, para que NO entren codsol solo de productos)
df_final = (
    df_last_estado
    .join(df_final_autonomias, on="CODSOLICITUD", how="left")
    .join(df_prod_snap, on="CODSOLICITUD", how="left")
)

# 8) PRODUCTO (TC/CEF) + MATSUPERIOR (desde orgánico usando MATANALISTA_FINAL + mes)
df_final = add_producto_tipo(df_final)


# 9) POWERAPPS (fallback SOLO si sigue NULL) + motivos
df_final = apply_powerapps_fallback(df_final, df_apps)

# 9.1) MATSUPERIOR (orgánico)
df_final = add_matsuperior_from_organico(df_final, df_org)

# 9.2) AUTONOMIA POR MONTO (crear si FLGAUTONOMIA=0, validar/corregir si FLGAUTONOMIA=1)
df_final = aplicar_reglas_autonomia_monto_y_validar(df_final)



AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `REGLAAUTONOMIA` cannot be resolved. Did you mean one of the following? [`FLGAUTONOMIA`, `MATAUTONOMIA`, `TS_AUTONOMIA`, `NIVELAUTONOMIA`, `PASO_AUTONOMIA`].;
