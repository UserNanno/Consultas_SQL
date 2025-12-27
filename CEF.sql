# =========================================================
# 9.5) REGLA DE MONTO + VALIDACION REGLA DE PASO + TRAZABILIDAD
# =========================================================

def parse_monto_to_double(col):
    """
    Convierte montos tipo "123,456.78" o "123.456,78" o "123456" a double.
    (Estrategia simple: remover espacios y separadores de miles comunes)
    """
    s = F.trim(col.cast("string"))
    s = F.regexp_replace(s, r"\s", "")
    # quita separador de miles más común (coma)
    s1 = F.regexp_replace(s, r",", "")
    # si venía con formato europeo "123.456,78" esto quedaría "123.45678"
    # por eso también quitamos puntos y dejamos solo el último separador decimal si existiera
    # (si tus montos ya vienen limpios, esto no afecta mayormente)
    s2 = F.regexp_replace(s1, r"\.", "")
    return s2.cast("double")

def apply_autonomia_regla_monto(df):
    # ---- 1) Monto numérico
    df = df.withColumn("MTOAPROBADO_DBL", parse_monto_to_double(F.col("MTOAPROBADO")))

    # ---- 2) Niveles -> orden (ANALISTA < SUPERVISOR < GERENTE)
    def nivel_to_num(col):
        return (
            F.when(col == "ANALISTA", F.lit(1))
             .when(col == "SUPERVISOR", F.lit(2))
             .when(col == "GERENTE", F.lit(3))
             .otherwise(F.lit(0))
        )

    def num_to_nivel(col):
        return (
            F.when(col == 1, F.lit("ANALISTA"))
             .when(col == 2, F.lit("SUPERVISOR"))
             .when(col == 3, F.lit("GERENTE"))
             .otherwise(F.lit(None))
        )

    # ---- 3) Nivel requerido por monto (si no excede umbrales => 0 / sin autonomía por monto)
    req_num = (
        F.when(F.col("MTOAPROBADO_DBL") > F.lit(240000.0), F.lit(3))
         .when(F.col("MTOAPROBADO_DBL") > F.lit(100000.0), F.lit(2))
         .otherwise(F.lit(0))
    )

    df = df.withColumn("NIVEL_REQ_MONTO_NUM", req_num)

    # ---- 4) Base (regla de paso) - PERO: si NIVELAUTONOMIA=ANALISTA => se ignora (como acordado)
    base_num_raw = nivel_to_num(F.col("NIVELAUTONOMIA"))
    base_num = F.when(F.col("NIVELAUTONOMIA") == "ANALISTA", F.lit(0)).otherwise(base_num_raw)

    df = (
        df.withColumn("NIVEL_BASE_PASO_NUM", base_num)
          .withColumn("FLG_BASE_PASO", F.when(base_num > 0, F.lit(1)).otherwise(F.lit(0)))
    )

    # ---- 5) Nivel final: máximo entre base y requerido por monto
    final_num = F.greatest(F.col("NIVEL_BASE_PASO_NUM"), F.col("NIVEL_REQ_MONTO_NUM"))
    df = df.withColumn("NIVEL_FINAL_NUM", final_num).withColumn("NIVELAUTONOMIA", num_to_nivel(final_num))

    # ---- 6) Setear MATAUTONOMIA final
    # - Si final se queda en base (paso), se conserva MATAUTONOMIA
    # - Si final viene de monto o escalamiento:
    #     - GERENTE => U17293
    #     - SUPERVISOR => MATSUPERIOR (del MATANALISTA_FINAL)
    mat_from_monto = (
        F.when(F.col("NIVEL_FINAL_NUM") == 3, F.lit("U17293"))
         .when(F.col("NIVEL_FINAL_NUM") == 2, F.col("MATSUPERIOR"))
         .otherwise(F.lit(None).cast("string"))
    )

    df = df.withColumn(
        "MATAUTONOMIA",
        F.when(
            (F.col("NIVEL_FINAL_NUM") == F.col("NIVEL_BASE_PASO_NUM")) & (F.col("NIVEL_BASE_PASO_NUM") > 0),
            F.col("MATAUTONOMIA")
        ).when(
            F.col("NIVEL_FINAL_NUM") > 0,
            mat_from_monto
        ).otherwise(F.lit(None).cast("string"))
    )

    # ---- 7) FLGAUTONOMIA final (1 si final_num>0, sino 0)
    df = df.withColumn("FLGAUTONOMIA", F.when(F.col("NIVEL_FINAL_NUM") > 0, F.lit(1)).otherwise(F.lit(0)))

    # ---- 8) Trazabilidad REGLAAUTONOMIA
    # - PASO: viene solo por regla de paso y monto no exige nada mayor
    # - MONTO: no había paso válido (o era ANALISTA) y se asignó por monto
    # - PASO+MONTO: había paso válido pero el monto exigía escalar (validación)
    df = df.withColumn(
        "REGLAAUTONOMIA",
        F.when(
            (F.col("NIVEL_BASE_PASO_NUM") > 0) & (F.col("NIVEL_REQ_MONTO_NUM") == 0),
            F.lit("PASO")
        ).when(
            (F.col("NIVEL_BASE_PASO_NUM") == 0) & (F.col("NIVEL_REQ_MONTO_NUM") > 0),
            F.lit("MONTO")
        ).when(
            (F.col("NIVEL_BASE_PASO_NUM") > 0) & (F.col("NIVEL_REQ_MONTO_NUM") > 0) &
            (F.col("NIVEL_REQ_MONTO_NUM") > F.col("NIVEL_BASE_PASO_NUM")),
            F.lit("PASO+MONTO")
        ).when(
            (F.col("NIVEL_BASE_PASO_NUM") > 0) & (F.col("NIVEL_REQ_MONTO_NUM") > 0) &
            (F.col("NIVEL_REQ_MONTO_NUM") <= F.col("NIVEL_BASE_PASO_NUM")),
            F.lit("PASO")
        ).otherwise(F.lit(None))
    )

    # ---- 9) Ajustar columnas “de paso” cuando la autonomía final NO proviene de paso
    # Si quedó por MONTO o PASO+MONTO (escalado), marcamos PASO_AUTONOMIA='REGLA_MONTO' y TS_AUTONOMIA=TS_PRODUCTOS
    # (si prefieres conservar el paso original cuando sea PASO+MONTO, dime y lo dejo mixto)
    df = df.withColumn(
        "PASO_AUTONOMIA",
        F.when(F.col("REGLAAUTONOMIA").isin("MONTO", "PASO+MONTO"), F.lit("REGLA_MONTO"))
         .otherwise(F.col("PASO_AUTONOMIA"))
    ).withColumn(
        "TS_AUTONOMIA",
        F.when(F.col("REGLAAUTONOMIA").isin("MONTO", "PASO+MONTO"), F.col("TS_PRODUCTOS"))
         .otherwise(F.col("TS_AUTONOMIA"))
    )

    # ---- 10) FLGAUTONOMIAOBSERVADA: solo aplica a regla de paso observada; si viene por monto, la bajamos a 0
    df = df.withColumn(
        "FLGAUTONOMIAOBSERVADA",
        F.when(F.col("REGLAAUTONOMIA").isin("MONTO", "PASO+MONTO"), F.lit(0))
         .otherwise(F.col("FLGAUTONOMIAOBSERVADA"))
    )

    # Limpieza auxiliares
    df = df.drop("MTOAPROBADO_DBL", "NIVEL_REQ_MONTO_NUM", "NIVEL_BASE_PASO_NUM", "NIVEL_FINAL_NUM", "FLG_BASE_PASO")
    return df


# === aplicar regla al final (ya con MTOAPROBADO y MATSUPERIOR listos) ===
df_final = apply_autonomia_regla_monto(df_final)








    "TS_AUTONOMIA",
    "REGLAAUTONOMIA",
