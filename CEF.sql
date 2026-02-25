from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from pyspark import StorageLevel

BASE_DIR_ORGANICO   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/ORGANICO/ORGANICO.csv"
PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/POWERAPPS/POWERAPP.csv"
PATH_SF_ESTADOS     = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"
PATH_SF_PRODUCTOS   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"

# HELPERS TEXTO / FECHAS
def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def limpiar_cesado(col):
    c = col.cast("string")
    c = F.regexp_replace(c, r"(?i)\(?\s*CESADO\s*\)?", "")
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)

def norm_txt(col):
    """
    Normalización robusta:
    - upper
    - quitar tildes
    - normalizar espacios
    - dejar letras/números/espacios
    """
    c = col.cast("string")
    c = F.upper(c)
    c = quitar_tildes(c)
    c = F.regexp_replace(c, u"\u00A0", " ")
    c = F.regexp_replace(c, u"\u202F", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    c = F.regexp_replace(c, r"[^A-Z0-9 ]", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    return c

def norm_col(df, cols):
    for c in cols:
        df = df.withColumn(c, norm_txt(F.col(c)))
    return df

def parse_fecha_hora_esp(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r"\s+", " ")
    s = F.regexp_replace(s, r"(?i)a\W*m\W*", "AM")
    s = F.regexp_replace(s, r"(?i)p\W*m\W*", "PM")
    return F.to_timestamp(s, "dd/MM/yyyy hh:mm a")

def to_decimal_monto(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.trim(s)
    s = F.regexp_replace(s, r"\s+", "")
    # robustez ante separadores
    s = F.when((F.instr(s, ",") > 0) & (F.instr(s, ".") > 0), F.regexp_replace(s, r"\.", "")).otherwise(s)
    s = F.when(F.instr(s, ",") > 0, F.regexp_replace(s, ",", ".")).otherwise(s)
    return s.cast(DecimalType(18, 2))

def norm_codmes(col):
    s = col.cast("string")
    s = F.regexp_replace(s, r"[^0-9]", "")
    s = F.when(F.length(s) >= 6, F.substring(s, 1, 6)).otherwise(s)
    return s



# LOADERS
def load_dicc_organico(spark, path_organico_csv):
    """
    ORGANICO.csv (diccionario):
    MATORGANICO;CODMES;NOMBRE;MATSUPERIOR;CORREO;NBRCORTO;TOKENS_MATCH;D_SIZE;O_SIZE;UNION_SIZE;JACCARD
    """
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")  # si lo grabaste en utf-8, cambia a utf-8
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_organico_csv)
        .select(
            F.col("CODMES").cast("string").alias("CODMES"),
            F.col("NOMBRE").alias("NOMBRE"),
            F.col("MATORGANICO").alias("MATORGANICO"),
            F.col("MATSUPERIOR").alias("MATSUPERIOR"),
            F.col("CORREO").alias("CORREO"),
            F.col("NBRCORTO").alias("NBRCORTO"),
            F.col("TOKENS_MATCH").alias("TOKENS_MATCH"),
            F.col("JACCARD").alias("JACCARD"),
        )
    )

    df = (
        df.withColumn("CODMES", norm_codmes(F.col("CODMES")))
          .withColumn("NOMBRE", limpiar_cesado(norm_txt(F.col("NOMBRE"))))
    )

    w = Window.partitionBy("CODMES", "NOMBRE").orderBy(
        F.col("TOKENS_MATCH").cast("int").desc_nulls_last(),
        F.col("JACCARD").cast("double").desc_nulls_last(),
        F.col("MATORGANICO").isNotNull().desc()
    )
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")
    return df




def load_powerapps_export(spark, path_apps_export):
    """
    POWERAPP.csv (export final):
    CODMES;CODSOLICITUD;FECHORACREACION;FECCREACION;HORACREACION;MATANALISTA;FECASIGNACION;PRODUCTO;
    RESULTADOANALISTA;MOTIVORESULTADOANALISTA;MOTIVOMALADERIVACION;SUBMOTIVOMALADERIVACION
    """
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "utf-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_apps_export)
        .select(
            F.col("CODMES").cast("string").alias("CODMES_APPS"),
            F.col("CODSOLICITUD"),
            F.col("FECHORACREACION"),
            F.col("MATANALISTA"),
            F.col("PRODUCTO"),
            F.col("RESULTADOANALISTA"),
            F.col("MOTIVORESULTADOANALISTA"),
            F.col("MOTIVOMALADERIVACION"),
            F.col("SUBMOTIVOMALADERIVACION"),
        )
    )

    df = (
        df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
          .withColumn("CODMES_APPS", norm_codmes(F.col("CODMES_APPS")))
    )

    df = norm_col(df, [
        "MATANALISTA", "PRODUCTO", "RESULTADOANALISTA",
        "MOTIVORESULTADOANALISTA", "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"
    ])

    df = df.withColumn("TS_APPS", F.to_timestamp(F.col("FECHORACREACION"), "yyyy-MM-dd HH:mm:ss"))

    w = Window.partitionBy("CODSOLICITUD").orderBy(F.col("TS_APPS").desc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

    return df



# LOADERS SALESFORCE
def load_sf_estados(spark, path_estados):
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_estados)
    )

    df = df_raw.select(
        F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION_RAW"),
        F.col("Fecha de finalización del paso").alias("FECFINEVALUACION_RAW"),
        F.col("Nombre del registro").alias("CODSOLICITUD"),
        F.col("Estado").alias("ESTADOSOLICITUD"),
        F.col("Paso: Nombre").alias("NBRPASO"),
        F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
        F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
        F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
        F.col("Estado del paso").alias("ESTADOSOLICITUDPASO"),
    )

    df = norm_col(df, [
        "ESTADOSOLICITUD", "ESTADOSOLICITUDPASO", "NBRPASO", "NBRULTACTOR", "NBRULTACTORPASO", "PROCESO"
    ])

    df = (
        df.withColumn("NBRULTACTOR", limpiar_cesado(F.col("NBRULTACTOR")))
          .withColumn("NBRULTACTORPASO", limpiar_cesado(F.col("NBRULTACTORPASO")))
    )

    df = (
        df.withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp(F.col("FECINICIOEVALUACION_RAW")))
          .withColumn("FECHORFINEVALUACION", parse_fecha_hora_esp(F.col("FECFINEVALUACION_RAW")))
          .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
          .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))
          .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
          .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))
          .withColumn("CODMESEVALUACION", F.date_format("FECINICIOEVALUACION", "yyyyMM").cast("string"))
          .drop("FECINICIOEVALUACION_RAW", "FECFINEVALUACION_RAW")
    )

    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
    return df




def load_sf_productos_validos(spark, path_productos):
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_productos)
    )

    df = df_raw.select(
        F.col("Nombre de la oportunidad").alias("CODSOLICITUD"),
        F.col("Nombre del Producto").alias("NBRPRODUCTO"),
        F.col("Etapa").alias("ETAPA"),
        F.col("Analista").alias("NBRANALISTA"),                    # para lookup MAT3
        F.col("Analista de crédito").alias("NBRANALISTAASIGNADO"), # para lookup MAT4
        F.col("Tipo de Acción").alias("TIPACCION"),
        F.col("Fecha de creación").alias("FECCREACION_RAW"),
        F.col("Monto/Línea Solicitud").alias("MTOSOLICITADO"),
        F.col("Monto/Línea aprobada").alias("MTOAPROBADO"),
        F.col("Monto Solicitado/Ofertado Divisa").alias("NBRDIVISAMTOOFERTADO"),
        F.col("Monto Solicitado/Ofertado").alias("MTOOFERTADO"),
        F.col("Divisa de la oportunidad").alias("NBRDIVISA"),
        F.col("Respuesta CDA").alias("RESPUESTACDA"),
        F.col("CEM Divisa").alias("NBRDIVISACEM"),
        F.col("CEM").alias("MTOCEM"),
        F.col("CEM créditos Divisa").alias("NBRDIVISACEMANALISTA"),
        F.col("CEM créditos").alias("MTOCEMANALISTA"),
        F.col("Tasa Anual (TEA) %").alias("TASAEFECTIVAANUAL")
    )

    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))

    df = df.withColumn(
        "FLG_CODSOLICITUD_VALIDO",
        F.when((F.length("CODSOLICITUD") == 11) & (F.col("CODSOLICITUD").startswith("O00")), 1).otherwise(0)
    ).filter(F.col("FLG_CODSOLICITUD_VALIDO") == 1).drop("FLG_CODSOLICITUD_VALIDO")

    df = norm_col(df, [
        "NBRPRODUCTO","ETAPA","NBRANALISTA","NBRANALISTAASIGNADO","TIPACCION","NBRDIVISAMTOOFERTADO",
        "NBRDIVISA","RESPUESTACDA","NBRDIVISACEM","NBRDIVISACEMANALISTA"
    ])

    df = (
        df.withColumn("NBRANALISTA", limpiar_cesado(F.col("NBRANALISTA")))
          .withColumn("NBRANALISTAASIGNADO", limpiar_cesado(F.col("NBRANALISTAASIGNADO")))
    )

    df = (
        df.withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION_RAW").cast("string")))
          .withColumn(
              "FECCREACION",
              F.coalesce(
                  F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                  F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                  F.to_date("FECCREACION_STR")
              )
          )
          .withColumn("CODMESCREACION", F.date_format("FECCREACION", "yyyyMM").cast("string"))
          .drop("FECCREACION_RAW", "FECCREACION_STR")
    )

    df = (
        df.withColumn("MTOSOLICITADO", to_decimal_monto(F.col("MTOSOLICITADO")))
          .withColumn("MTOAPROBADO", to_decimal_monto(F.col("MTOAPROBADO")))
          .withColumn("MTOOFERTADO", to_decimal_monto(F.col("MTOOFERTADO")))
          .withColumn("MTOCEM", to_decimal_monto(F.col("MTOCEM")))
          .withColumn("MTOCEMANALISTA", to_decimal_monto(F.col("MTOCEMANALISTA")))
          .withColumn("TASAEFECTIVAANUAL", to_decimal_monto(F.col("TASAEFECTIVAANUAL")))
    )

    return df








# ENRICHMENT SIN TOKENS (LOOKUP EXACTO)
def enrich_estados_con_dicc(df_estados, df_dicc):
    dicc = df_dicc.select(
        F.col("CODMES").alias("CODMESEVALUACION"),
        F.col("NOMBRE"),
        F.col("MATORGANICO"),
        F.col("MATSUPERIOR"),
    )

    df = (
        df_estados
        .withColumn("NBRULTACTOR", limpiar_cesado(F.col("NBRULTACTOR")))
        .withColumn("NBRULTACTORPASO", limpiar_cesado(F.col("NBRULTACTORPASO")))
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRULTACTOR")
            .withColumnRenamed("MATORGANICO", "MATORGANICO")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIOR"),
        on=["CODMESEVALUACION", "NBRULTACTOR"],
        how="left"
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRULTACTORPASO")
            .withColumnRenamed("MATORGANICO", "MATORGANICOPASO")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIORPASO"),
        on=["CODMESEVALUACION", "NBRULTACTORPASO"],
        how="left"
    )

    return df

def enrich_productos_con_dicc(df_productos, df_dicc):
    dicc = df_dicc.select(
        F.col("CODMES").alias("CODMESCREACION"),
        F.col("NOMBRE"),
        F.col("MATORGANICO"),
        F.col("MATSUPERIOR"),
    )

    df = (
        df_productos
        .withColumn("NBRANALISTA", limpiar_cesado(F.col("NBRANALISTA")))
        .withColumn("NBRANALISTAASIGNADO", limpiar_cesado(F.col("NBRANALISTAASIGNADO")))
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRANALISTA")
            .withColumnRenamed("MATORGANICO", "MATORGANICO_ANALISTA")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIOR_ANALISTA"),
        on=["CODMESCREACION", "NBRANALISTA"],
        how="left"
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRANALISTAASIGNADO")
            .withColumnRenamed("MATORGANICO", "MATORGANICO_ASIGNADO")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIOR_ASIGNADO"),
        on=["CODMESCREACION", "NBRANALISTAASIGNADO"],
        how="left"
    )

    return df





# SNAPSHOTS / REGLAS
def build_estado_analista_snapshot(df_estados_enriq):
    is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
    is_cef = F.col("PROCESO").isin(
        "CO SOLICITUD APROBACIONES TLMK",
        "SFCP APROBACIONES EDUCATIVO",
        "CO SOLICITUD APROBACIONES"
    )

    paso_tc_analista  = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
    paso_cef_analista = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")
    es_paso_analista = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

    df_paso = df_estados_enriq.filter(es_paso_analista)

    w_evt = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    df_last_evt = (
        df_paso
        .withColumn("rn_evt", F.row_number().over(w_evt))
        .filter(F.col("rn_evt") == 1)
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_PASO_ANALISTA")
        )
    )

    df_fuerte = df_paso.filter(F.col("ESTADOSOLICITUDPASO").isin("APROBADO", "RECHAZADO"))
    df_last_fuerte = (
        df_fuerte
        .withColumn("rn", F.row_number().over(w_evt))
        .filter(F.col("rn") == 1)
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_FUERTE"),
            F.col("FECHORINICIOEVALUACION").alias("TS_FUERTE")
        )
    )

    df_rec = df_paso.filter(F.col("ESTADOSOLICITUDPASO") == "RECUPERADA")
    df_last_rec = (
        df_rec
        .withColumn("rn", F.row_number().over(w_evt))
        .filter(F.col("rn") == 1)
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_REC"),
            F.col("FECHORINICIOEVALUACION").alias("TS_REC")
        )
    )

    out = (
        df_last_evt
        .join(df_last_fuerte, on="CODSOLICITUD", how="left")
        .join(df_last_rec,   on="CODSOLICITUD", how="left")
        .withColumn(
            "ESTADOSOLICITUDANALISTA",
            F.when(F.col("ESTADO_FUERTE").isNotNull(), F.col("ESTADO_FUERTE"))
             .when(F.col("ESTADO_REC").isNotNull(),    F.col("ESTADO_REC"))
             .otherwise(F.lit("PENDIENTE"))
        )
        .withColumn(
            "TS_DECISION_ANALISTA",
            F.when(F.col("ESTADO_FUERTE").isNotNull(), F.col("TS_FUERTE"))
             .when(F.col("ESTADO_REC").isNotNull(),    F.col("TS_REC"))
             .otherwise(F.lit(None).cast("timestamp"))
        )
        .drop("ESTADO_FUERTE", "TS_FUERTE", "ESTADO_REC", "TS_REC")
    )

    return out




def build_last_estado_snapshot(df_estados_enriq):
    w_last = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    return (
        df_estados_enriq
        .withColumn("rn_last", F.row_number().over(w_last))
        .filter(F.col("rn_last") == 1)
        .select(
            "CODSOLICITUD",
            "PROCESO",
            "ESTADOSOLICITUD",
            "ESTADOSOLICITUDPASO",
            F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_ESTADOS"),
            F.col("FECHORFINEVALUACION").alias("TS_FIN_ULTIMO_EVENTO_ESTADOS"),
            F.to_date("FECHORINICIOEVALUACION").alias("FECINICIOEVALUACION_ULT"),
            F.to_date("FECHORFINEVALUACION").alias("FECFINEVALUACION_ULT"),
            F.date_format(F.to_date("FECHORINICIOEVALUACION"), "yyyyMM").cast("string").alias("CODMESEVALUACION"),
        )
        .drop("rn_last")
    )




def build_productos_snapshot(df_productos_enriq):
    w_prod = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECCREACION").desc_nulls_last())
    return (
        df_productos_enriq
        .withColumn("rn_prod", F.row_number().over(w_prod))
        .filter(F.col("rn_prod") == 1)
        .select(
            "CODSOLICITUD",
            "NBRPRODUCTO",
            "ETAPA",
            "TIPACCION",
            "NBRDIVISA",
            "MTOSOLICITADO",
            "MTOAPROBADO",
            "NBRDIVISAMTOOFERTADO",
            "MTOOFERTADO",
            "RESPUESTACDA",
            "NBRDIVISACEM",
            "MTOCEM",
            "NBRDIVISACEMANALISTA",
            "MTOCEMANALISTA",
            "TASAEFECTIVAANUAL",
            F.col("FECCREACION").alias("TS_PRODUCTOS"),
            "MATORGANICO_ANALISTA",
            "MATORGANICO_ASIGNADO",
        )
        .drop("rn_prod")
    )




def add_producto_tipo(df_final):
    is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
    is_cef = F.col("PROCESO").isin(
        "CO SOLICITUD APROBACIONES TLMK",
        "SFCP APROBACIONES EDUCATIVO",
        "CO SOLICITUD APROBACIONES"
    )
    return df_final.withColumn(
        "PRODUCTO",
        F.when(is_tc, F.lit("TC"))
         .when(is_cef, F.lit("CEF"))
         .otherwise(F.lit(None))
    )






def build_matanalista_final(df_estados_enriq, df_prod_snap):
    gerente = ["U17293"]
    supervisores = ["U17560", "U13421", "S18795", "U18900", "E12624", "U23436"]
    roles_excluidos = gerente + supervisores

    def es_valido_analista(col):
        return (col.isNotNull()) & (~col.isin(roles_excluidos))

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

    df_prod_mats = df_prod_snap.select(
        "CODSOLICITUD",
        F.col("MATORGANICO_ANALISTA").alias("MAT3"),
        F.col("MATORGANICO_ASIGNADO").alias("MAT4")
    )

    df_all = df_base_latest.join(df_prod_mats, on="CODSOLICITUD", how="left")

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
        .select("CODSOLICITUD", "TS_BASE_ESTADOS", "MATANALISTA_FINAL", "ORIGEN_MATANALISTA")
    )

    return df_final





def add_matsuperior_from_dicc(df_final, df_dicc):
    df_dicc_key = (
        df_dicc
        .select(
            F.col("CODMES").cast("string").alias("CODMESEVALUACION"),
            F.col("MATORGANICO").alias("MATANALISTA_FINAL"),
            F.col("MATSUPERIOR").alias("MATSUPERIOR_ORG")
        )
        .dropDuplicates(["CODMESEVALUACION", "MATANALISTA_FINAL"])
    )

    df_out = df_final.join(df_dicc_key, on=["CODMESEVALUACION", "MATANALISTA_FINAL"], how="left")

    if "MATSUPERIOR" in df_out.columns:
        df_out = (
            df_out
            .withColumn("MATSUPERIOR", F.coalesce(F.col("MATSUPERIOR"), F.col("MATSUPERIOR_ORG")))
            .drop("MATSUPERIOR_ORG")
        )
    else:
        df_out = df_out.withColumnRenamed("MATSUPERIOR_ORG", "MATSUPERIOR")

    return df_out







def apply_powerapps_fallback(df_final, df_apps):
    df_apps_sel = (
        df_apps
        .select(
            "CODSOLICITUD",
            F.col("MATANALISTA").alias("MATANALISTA_APPS"),
            F.col("RESULTADOANALISTA").alias("RESULTADOANALISTA_APPS"),
            F.col("PRODUCTO").alias("PRODUCTO_APPS"),
            "RESULTADOANALISTA",
            "MOTIVORESULTADOANALISTA",
            "MOTIVOMALADERIVACION",
            "SUBMOTIVOMALADERIVACION",
        )
        .dropDuplicates(["CODSOLICITUD"])
    )

    df_out = df_final.join(df_apps_sel, on="CODSOLICITUD", how="left")

    cond_ambos_no_nulos = F.col("MATANALISTA_FINAL").isNotNull() & F.col("MATANALISTA_APPS").isNotNull()
    cond_no_coinciden   = F.col("MATANALISTA_FINAL") != F.col("MATANALISTA_APPS")
    cond_override       = cond_ambos_no_nulos & cond_no_coinciden

    df_out = (
        df_out
        .withColumn(
            "MATANALISTA_FINAL",
            F.when(cond_override, F.col("MATANALISTA_APPS"))
             .otherwise(F.coalesce(F.col("MATANALISTA_FINAL"), F.col("MATANALISTA_APPS")))
        )
        .withColumn(
            "ORIGEN_MATANALISTA",
            F.when(cond_override, F.lit("APPS_OVERRIDE"))
             .when(F.col("ORIGEN_MATANALISTA").isNull() & F.col("MATANALISTA_APPS").isNotNull(), F.lit("APPS_MAT"))
             .otherwise(F.col("ORIGEN_MATANALISTA"))
        )
        .withColumn("ESTADOSOLICITUDPASO", F.coalesce(F.col("ESTADOSOLICITUDPASO"), F.col("RESULTADOANALISTA_APPS")))
        .withColumn("NBRPRODUCTO",        F.coalesce(F.col("NBRPRODUCTO"),        F.col("PRODUCTO_APPS")))
        .drop("MATANALISTA_APPS", "RESULTADOANALISTA_APPS", "PRODUCTO_APPS")
    )

    return df_out






def build_atencion_analista_primera_fmt(df_estados_enriq):
    paso_tc_analista  = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
    paso_cef_analista = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")

    df_paso = df_estados_enriq.filter(paso_tc_analista | paso_cef_analista)

    w_first = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").asc_nulls_last(),
        F.col("FECHORFINEVALUACION").asc_nulls_last()
    )

    df_first = (
        df_paso
        .withColumn("rn", F.row_number().over(w_first))
        .filter(F.col("rn") == 1)
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_LLEGA_ANALISTA"),
            F.col("FECHORFINEVALUACION").alias("TS_FIN_ANALISTA"),
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_ANALISTA")
        )
    )

    df_first = (
        df_first
        .withColumn("FH_LLEGA_ANALISTA", F.date_format("TS_LLEGA_ANALISTA", "yyyy-MM-dd HH:mm"))
        .withColumn("FH_FIN_ANALISTA",   F.date_format("TS_FIN_ANALISTA",   "yyyy-MM-dd HH:mm"))
    )

    return df_first





# EJECUCIÓN
# 12.1 Staging
df_dicc     = load_dicc_organico(spark, BASE_DIR_ORGANICO)
df_estados  = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos= load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps     = load_powerapps_export(spark, PATH_PA_SOLICITUDES)

# 2) Enriquecimiento (lookup exacto)
df_estados_enriq     = enrich_estados_con_dicc(df_estados, df_dicc)
df_atencion_analista = build_atencion_analista_primera_fmt(df_estados_enriq)
df_productos_enriq   = enrich_productos_con_dicc(df_productos, df_dicc)

# 3) Snapshots
df_estado_analista = build_estado_analista_snapshot(df_estados_enriq)
df_last_estado     = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap       = build_productos_snapshot(df_productos_enriq)

# 3.1) Adjuntar estado analista al snapshot base
df_last_estado = df_last_estado.join(df_estado_analista, on="CODSOLICITUD", how="left")

# 4) Atribución matanalista final
df_matanalista = build_matanalista_final(df_estados_enriq, df_prod_snap)

# 5) Ensamble final (base = último estado)
df_final = (
    df_last_estado
    .join(df_atencion_analista, on="CODSOLICITUD", how="left")
    .join(df_matanalista, on="CODSOLICITUD", how="left")
    .join(df_prod_snap.select(
        "CODSOLICITUD",
        "NBRPRODUCTO","ETAPA","TIPACCION","NBRDIVISA",
        "MTOSOLICITADO","MTOAPROBADO","NBRDIVISAMTOOFERTADO","MTOOFERTADO",
        "RESPUESTACDA","NBRDIVISACEM","MTOCEM","NBRDIVISACEMANALISTA","MTOCEMANALISTA",
        "TASAEFECTIVAANUAL", "TS_PRODUCTOS"
    ), on="CODSOLICITUD", how="left")
)

# 6) Producto (TC/CEF)
df_final = add_producto_tipo(df_final)

# 7) MATSUPERIOR por diccionario (mes + matrícula)
df_final = add_matsuperior_from_dicc(df_final, df_dicc)

# 8) PowerApps fallback + motivos
df_final = apply_powerapps_fallback(df_final, df_apps)

# 9) Re-llenar MATSUPERIOR por si PowerApps completó MATANALISTA_FINAL
df_final = add_matsuperior_from_dicc(df_final, df_dicc)

# 10) Selección final (sin autonomías)
df_final = df_final.select(
    "CODSOLICITUD",
    "PRODUCTO",
    "CODMESEVALUACION",
    "FH_LLEGA_ANALISTA",
    "FH_FIN_ANALISTA",
    "ESTADO_ANALISTA",

    "MATANALISTA_FINAL",
    "ORIGEN_MATANALISTA",
    "MATSUPERIOR",

    "PROCESO",
    "ESTADOSOLICITUD",
    "ESTADOSOLICITUDPASO",
    "ESTADOSOLICITUDANALISTA",

    "NBRPRODUCTO",
    "ETAPA",
    "TIPACCION",
    "NBRDIVISA",
    "MTOSOLICITADO",
    "MTOAPROBADO",
    "MTOOFERTADO",
    "RESPUESTACDA",
    "NBRDIVISACEM",
    "MTOCEM",
    "NBRDIVISACEMANALISTA",
    "MTOCEMANALISTA",
    "TASAEFECTIVAANUAL",
    "RESULTADOANALISTA",
    "MOTIVORESULTADOANALISTA",
    "MOTIVOMALADERIVACION",
    "SUBMOTIVOMALADERIVACION",
)




df_out = df_final.persist(StorageLevel.DISK_ONLY)
_ = df_out.count()



(df_out.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.TP_SOLICITUDES_CENTRALIZADO")
)




df_out.unpersist()
