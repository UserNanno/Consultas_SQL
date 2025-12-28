%md
## 9. Atribución del analista  
### MATANALISTA_FINAL y ORIGEN_MATANALISTA

La atribución del analista se resuelve combinando múltiples señales debido a
inconsistencias reales del flujo operativo en Salesforce.

La lógica evalúa **secuencialmente** cuatro posibles fuentes de matrícula,
aceptando únicamente matrículas **válidas de analista** y excluyendo
matrículas correspondientes a **supervisores o gerente**.

**Orden de prioridad de fuentes:**
1. **Estados**
   - MAT1: desde NBRULTACTOR
   - MAT2: desde NBRULTACTORPASO
2. **Productos**
   - MAT3: desde NBRANALISTA
   - MAT4: desde NBRANALISTAASIGNADO

**Regla de exclusión por rol:**
- Si la matrícula evaluada corresponde a un **supervisor** o **gerente**,
  se descarta y se continúa con la siguiente fuente.
- Solo se acepta la **primera matrícula válida** según el orden definido.
- Si ninguna fuente contiene una matrícula válida, el resultado es **NULL**.

**Campos generados:**
- `MATANALISTA_FINAL`: matrícula del analista atribuida a la solicitud.
- `ORIGEN_MATANALISTA`: fuente desde la cual se obtuvo la matrícula final
  (ESTADOS_MAT1, ESTADOS_MAT2, PRODUCTOS_MAT3, PRODUCTOS_MAT4).

Esta lógica protege la atribución frente a casos donde el actor del paso
corresponde a un supervisor o gerente por errores o particularidades del flujo.






from pyspark.sql import functions as F
from pyspark.sql.window import Window

def build_matanalista_final(df_estados_enriq, df_prod_snap):

    # -------------------------
    # Roles a excluir
    # -------------------------
    gerente = ["U17293"]
    supervisores = ["U17560", "U13421", "S18795", "U18900", "E12624", "U23436"]
    roles_excluidos = gerente + supervisores

    def es_valido_analista(col):
        return (col.isNotNull()) & (~col.isin(roles_excluidos))

    # -------------------------
    # 1) Base desde ESTADOS
    # -------------------------
    is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
    is_cef = F.col("PROCESO").isin(
        "CO SOLICITUD APROBACIONES TLMK",
        "SFCP APROBACIONES EDUCATIVO",
        "CO SOLICITUD APROBACIONES"
    )

    paso_tc_analista  = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
    paso_cef_analista = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")

    es_paso_base = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

    w_base = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last()
    )

    df_base_latest = (
        df_estados_enriq
        .filter(es_paso_base)
        .withColumn("rn", F.row_number().over(w_base))
        .filter(F.col("rn") == 1)
        .select(
            "CODSOLICITUD",
            F.col("MATORGANICO").alias("MAT1"),
            F.col("MATORGANICOPASO").alias("MAT2"),
            F.col("FECHORINICIOEVALUACION").alias("TS_BASE_ESTADOS")
        )
        .drop("rn")
    )

    # -------------------------
    # 2) Productos (MAT3 / MAT4)
    # -------------------------
    df_prod_mats = (
        df_prod_snap
        .select(
            "CODSOLICITUD",
            F.col("MATORGANICO_ANALISTA").alias("MAT3"),
            F.col("MATORGANICO_ASIGNADO").alias("MAT4")
        )
    )

    df_all = df_base_latest.join(df_prod_mats, on="CODSOLICITUD", how="left")

    # -------------------------
    # 3) Selección secuencial con exclusión por rol
    # -------------------------
    df_final = (
        df_all
        .withColumn(
            "MATANALISTA_FINAL",
            F.when(es_valido_analista(F.col("MAT1")), F.col("MAT1"))
             .when(es_valido_analista(F.col("MAT2")), F.col("MAT2"))
             .when(es_valido_analista(F.col("MAT3")), F.col("MAT3"))
             .when(es_valido_analista(F.col("MAT4")), F.col("MAT4"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "ORIGEN_MATANALISTA",
            F.when(es_valido_analista(F.col("MAT1")), F.lit("ESTADOS_MAT1"))
             .when(es_valido_analista(F.col("MAT2")), F.lit("ESTADOS_MAT2"))
             .when(es_valido_analista(F.col("MAT3")), F.lit("PRODUCTOS_MAT3"))
             .when(es_valido_analista(F.col("MAT4")), F.lit("PRODUCTOS_MAT4"))
             .otherwise(F.lit(None))
        )
        .select(
            "CODSOLICITUD",
            "TS_BASE_ESTADOS",
            "MATANALISTA_FINAL",
            "ORIGEN_MATANALISTA"
        )
    )

    return df_final
