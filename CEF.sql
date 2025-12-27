from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
    
# =========================================================
# PATHS
# =========================================================
BASE_DIR_ORGANICO = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/1n_Activos_*.csv"
PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/BASESOLICITUDES/POWERAPP_EDV.csv"
PATH_SF_ESTADOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"
PATH_SF_PRODUCTOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"

# =========================================================
# NORMALIZACION
# =========================================================
def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def norm_txt(col):
    return F.trim(quitar_tildes(F.upper(col)))

def norm_col(df, cols):
    for c in cols:
        df = df.withColumn(c, norm_txt(F.col(c)))
    return df

def limpiar_cesado(col):
    return F.trim(F.regexp_replace(col, r"\(CESADO\)", ""))

# =========================================================
# FECHA/HORA ESP
# =========================================================
def parse_fecha_hora_esp(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r"\s+", " ")
    s = F.regexp_replace(s, r"(?i)a\W*m\W*", "AM")
    s = F.regexp_replace(s, r"(?i)p\W*m\W*", "PM")
    return F.to_timestamp(s, "dd/MM/yyyy hh:mm a")

# =========================================================
# TOKENS ORG + MATCH POR TOKENS
# =========================================================
def build_org_tokens(df_org):
    df_org = (
        df_org
        .withColumn("TOKENS_NOMBRE_ORG", F.array_distinct(F.split(F.col("NOMBRECOMPLETO_CLEAN"), r"\s+")))
        .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
    )
    return (
        df_org
        .select("CODMES", "MATORGANICO", "MATSUPERIOR", "NOMBRECOMPLETO", "TOKENS_NOMBRE_ORG", "N_TOK_ORG")
        .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_ORG"))
    )

def match_persona_vs_organico(df_org_tokens, df_sf, codmes_sf_col, codsol_col, nombre_sf_col,
                              min_tokens=3, min_ratio_sf=0.60):
    df_sf_nombres = (
        df_sf
        .withColumn("TOKENS_NOMBRE_SF", F.array_distinct(F.split(F.col(nombre_sf_col), r"\s+")))
        .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
    )

    df_sf_tokens = (
        df_sf_nombres
        .select(codmes_sf_col, codsol_col, nombre_sf_col, "TOKENS_NOMBRE_SF", "N_TOK_SF")
        .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF"))
    )

    df_join = (
        df_sf_tokens.alias("sf")
        .join(
            df_org_tokens.alias("org"),
            (F.col(f"sf.{codmes_sf_col}") == F.col("org.CODMES")) &
            (F.col("sf.TOKEN") == F.col("org.TOKEN")),
            "inner"
        )
    )

    match_cols = [codmes_sf_col, codsol_col, nombre_sf_col, "MATORGANICO", "MATSUPERIOR"]

    df_scores = (
        df_join
        .groupBy(match_cols)
        .agg(
            F.countDistinct("sf.TOKEN").alias("TOKENS_MATCH"),
            F.first("sf.N_TOK_SF").alias("N_TOK_SF"),
            F.first("org.N_TOK_ORG").alias("N_TOK_ORG"),
        )
        .withColumn("RATIO_SF", F.col("TOKENS_MATCH") / F.col("N_TOK_SF"))
        .withColumn("RATIO_ORG", F.col("TOKENS_MATCH") / F.col("N_TOK_ORG"))
        .filter((F.col("TOKENS_MATCH") >= min_tokens) & (F.col("RATIO_SF") >= min_ratio_sf))
    )

    # best match por NOMBRE (no “aplastar” todo por solicitud)
    w = Window.partitionBy(codmes_sf_col, codsol_col, nombre_sf_col).orderBy(
        F.col("TOKENS_MATCH").desc(),
        F.col("RATIO_SF").desc(),
        F.col("RATIO_ORG").desc(),
        F.col("MATORGANICO").asc()
    )

    return (
        df_scores
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
















# =========================================================
# LOADERS
# =========================================================
def load_organico(spark, path_organico):
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_organico)
    )

    df = df_raw.select(
        F.col("CODMES").cast("string").alias("CODMES"),
        F.col("Matrícula").alias("MATORGANICO"),
        F.col("Nombre Completo").alias("NOMBRECOMPLETO"),
        F.col("Correo electronico").alias("CORREO"),
        F.col("Fecha Ingreso").alias("FECINGRESO"),
        F.col("Matrícula Superior").alias("MATSUPERIOR"),
    )

    df = norm_col(df, ["MATORGANICO", "NOMBRECOMPLETO", "MATSUPERIOR"])
    df = df.withColumn("MATSUPERIOR", F.regexp_replace("MATSUPERIOR", r"^0(?=[A-Z]\d{5})", ""))

    df = (
        df.withColumn("FECINGRESO", F.to_date("FECINGRESO"))
          .withColumn("NOMBRECOMPLETO_CLEAN", limpiar_cesado(F.col("NOMBRECOMPLETO")))
    )
    return df

def load_sf_estados(spark, path_estados):
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_estados)
    )

    df = df_raw.select(
        F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION"),
        F.col("Fecha de finalización del paso").alias("FECFINEVALUACION"),
        F.col("Nombre del registro").alias("CODSOLICITUD"),
        F.col("Estado").alias("ESTADOSOLICITUD"),
        F.col("Paso: Nombre").alias("NBRPASO"),
        F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
        F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
        F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
        F.col("Estado del paso").alias("ESTADOSOLICITUDPASO"),
    )

    df = norm_col(df, ["ESTADOSOLICITUD", "ESTADOSOLICITUDPASO", "NBRPASO", "NBRULTACTOR", "NBRULTACTORPASO", "PROCESO"])

    df = (
        df.withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp(F.col("FECINICIOEVALUACION")))
          .withColumn("FECHORFINEVALUACION", parse_fecha_hora_esp(F.col("FECFINEVALUACION")))
          .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
          .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))
          .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
          .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))
          .withColumn("CODMESEVALUACION", F.date_format("FECINICIOEVALUACION", "yyyyMM").cast("string"))
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
        F.col("Analista").alias("NBRANALISTA"),  # (MAT3)
        F.col("Analista de crédito").alias("NBRANALISTAASIGNADO"),  # (MAT4) <-- NUEVO
        F.col("Tipo de Acción").alias("TIPACCION"),
        F.col("Fecha de creación").alias("FECCREACION"),
        F.col("Divisa de la oportunidad").alias("NBRDIVISA"),
        F.col("Monto/Línea Solicitud").alias("MTOSOLICITADO"),
        F.col("Monto/Línea aprobada").alias("MTOAPROBADO"),
        F.col("Monto Solicitado/Ofertado").alias("MTOOFERTADO"),
        F.col("Monto desembolsado").alias("MTODESEMBOLSADO"),
        F.col("Centralizado/Punto de Contacto").alias("CENTROATENCION")
    )

    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))

    df = df.withColumn(
        "FLG_CODSOLICITUD_VALIDO",
        F.when((F.length("CODSOLICITUD") == 11) & (F.col("CODSOLICITUD").startswith("O00")), 1).otherwise(0)
    )

    df = df.filter(F.col("FLG_CODSOLICITUD_VALIDO") == 1).drop("FLG_CODSOLICITUD_VALIDO")

    # Normaliza también el nuevo campo
    df = norm_col(df, ["NBRPRODUCTO","ETAPA","NBRANALISTA","NBRANALISTAASIGNADO","TIPACCION","NBRDIVISA","CENTROATENCION"])

    df = (
        df.withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION").cast("string")))
          .withColumn(
              "FECCREACION_DATE",
              F.coalesce(
                  F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                  F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                  F.to_date("FECCREACION_STR")
              )
          )
          .withColumn("FECCREACION", F.col("FECCREACION_DATE"))
          .withColumn("CODMESCREACION", F.date_format("FECCREACION", "yyyyMM").cast("string"))
          .drop("FECCREACION_STR","FECCREACION_DATE")
    )

    return df

def load_powerapps(spark, path_apps):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "utf-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_apps)
        .select(
            F.col("CODMES").cast("string").alias("CODMES_APPS"),
            "CODSOLICITUD",
            "FECHORACREACION",
            "MATANALISTA",
            "PRODUCTO",
            "RESULTADOANALISTA",
            "MOTIVORESULTADOANALISTA",
            "MOTIVOMALADERIVACION",
            "SUBMOTIVOMALADERIVACION",
        )
    )

    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
    # (opcional) normalizar textos útiles
    df = norm_col(df, ["MATANALISTA", "PRODUCTO", "RESULTADOANALISTA", "MOTIVORESULTADOANALISTA", "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"])

    w = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORACREACION").desc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")
    return df












# =========================================================
# ENRIQUECIMIENTO CON ORGANICO (fila-a-fila por actor)
# =========================================================
def enrich_estados_con_organico(df_estados, df_org_tokens):
    bm_actor = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_estados,
        codmes_sf_col="CODMESEVALUACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRULTACTOR",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    bm_paso = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_estados,
        codmes_sf_col="CODMESEVALUACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRULTACTORPASO",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    return (
        df_estados
        .join(
            bm_actor.select("CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTOR", "MATORGANICO", "MATSUPERIOR"),
            on=["CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTOR"],
            how="left"
        )
        .join(
            bm_paso.select(
                "CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTORPASO",
                F.col("MATORGANICO").alias("MATORGANICOPASO"),
                F.col("MATSUPERIOR").alias("MATSUPERIORPASO"),
            ),
            on=["CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTORPASO"],
            how="left"
        )
    )

def enrich_productos_con_organico(df_productos, df_org_tokens):
    # MAT3: por NBRANALISTA
    bm_analista = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_productos,
        codmes_sf_col="CODMESCREACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRANALISTA",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    # MAT4: por NBRANALISTAASIGNADO
    bm_asignado = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_productos,
        codmes_sf_col="CODMESCREACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRANALISTAASIGNADO",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    df_enriq = (
        df_productos
        .join(
            bm_analista.select(
                "CODMESCREACION","CODSOLICITUD",
                F.col("MATORGANICO").alias("MATORGANICO_ANALISTA"),      # MAT3
                F.col("MATSUPERIOR").alias("MATSUPERIOR_ANALISTA")
            ),
            on=["CODMESCREACION","CODSOLICITUD"],
            how="left"
        )
        .join(
            bm_asignado.select(
                "CODMESCREACION","CODSOLICITUD",
                F.col("MATORGANICO").alias("MATORGANICO_ASIGNADO"),      # MAT4
                F.col("MATSUPERIOR").alias("MATSUPERIOR_ASIGNADO")
            ),
            on=["CODMESCREACION","CODSOLICITUD"],
            how="left"
        )
    )

    return df_enriq











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












# =========================================================
# SNAPSHOTS (1 fila por CODSOLICITUD)
# =========================================================
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
            F.col("PROCESO").alias("PROCESO"),
            F.col("ESTADOSOLICITUD").alias("ESTADOSOLICITUD"),
            F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUDPASO"),
            F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_ESTADOS"),
            F.col("FECINICIOEVALUACION").alias("FECINICIOEVALUACION_ULT"),
            F.date_format(F.col("FECINICIOEVALUACION"), "yyyyMM").cast("string").alias("CODMESEVALUACION"),
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
            "MTOOFERTADO",
            "MTODESEMBOLSADO",
            F.col("FECCREACION").alias("TS_PRODUCTOS"),
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

def add_matsuperior_from_organico(df_final, df_org):
    df_org_key = (
        df_org
        .select(
            F.col("CODMES").cast("string").alias("CODMESEVALUACION"),
            F.col("MATORGANICO").alias("MATANALISTA_FINAL"),
            F.col("MATSUPERIOR").alias("MATSUPERIOR")
        )
        .dropDuplicates(["CODMESEVALUACION", "MATANALISTA_FINAL"])
    )
    return df_final.join(df_org_key, on=["CODMESEVALUACION", "MATANALISTA_FINAL"], how="left")












# =========================================================
# POWERAPPS APPLY (fallback + campos motivo)
# =========================================================
def apply_powerapps_fallback(df_final, df_apps):
    """
    Reglas:
    - MATANALISTA_FINAL: si null -> MATANALISTA (apps)
    - ESTADOSOLICITUDPASO: si null -> RESULTADOANALISTA (apps)
    - NBRPRODUCTO: si null -> PRODUCTO (apps)
    - ORIGEN_MATANALISTA: si null y MATANALISTA_APPS no null -> 'APPS_MAT'
    - Traer MOTIVORESULTADOANALISTA, MOTIVOMALADERIVACION, SUBMOTIVOMALADERIVACION
    """
    df_apps_sel = (
        df_apps
        .select(
            "CODSOLICITUD",
            F.col("MATANALISTA").alias("MATANALISTA_APPS"),
            F.col("RESULTADOANALISTA").alias("RESULTADOANALISTA_APPS"),
            F.col("PRODUCTO").alias("PRODUCTO_APPS"),
            "MOTIVORESULTADOANALISTA",
            "MOTIVOMALADERIVACION",
            "SUBMOTIVOMALADERIVACION",
        )
        .dropDuplicates(["CODSOLICITUD"])
    )

    df_out = (
        df_final
        .join(df_apps_sel, on="CODSOLICITUD", how="left")
        .withColumn("MATANALISTA_FINAL", F.coalesce(F.col("MATANALISTA_FINAL"), F.col("MATANALISTA_APPS")))
        .withColumn("ESTADOSOLICITUDPASO", F.coalesce(F.col("ESTADOSOLICITUDPASO"), F.col("RESULTADOANALISTA_APPS")))
        .withColumn("NBRPRODUCTO", F.coalesce(F.col("NBRPRODUCTO"), F.col("PRODUCTO_APPS")))
        .withColumn(
            "ORIGEN_MATANALISTA",
            F.when(F.col("ORIGEN_MATANALISTA").isNull() & F.col("MATANALISTA_APPS").isNotNull(), F.lit("APPS_MAT"))
             .otherwise(F.col("ORIGEN_MATANALISTA"))
        )
        .drop("MATANALISTA_APPS", "RESULTADOANALISTA_APPS", "PRODUCTO_APPS")
    )
    return df_out



from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

def to_decimal_monto(col):
    # soporta "108000.00" y también "108,000.00" o "108000,00" o "108.000,00"
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.trim(s)

    # Detecta formato:
    # - si tiene "," y "." -> asumimos "." miles y "," decimal => quitar "." y cambiar "," a "."
    # - si solo tiene "," -> asumimos "," decimal => cambiar "," a "."
    # - si solo tiene "." -> asumimos "." decimal (dejar)
    s = F.when(
            (F.instr(s, ",") > 0) & (F.instr(s, ".") > 0),
            F.regexp_replace(s, r"\.", "")  # quita puntos (miles)
        ).otherwise(s)

    s = F.when(F.instr(s, ",") > 0, F.regexp_replace(s, ",", ".")).otherwise(s)

    # quita espacios
    s = F.regexp_replace(s, r"\s+", "")

    return s.cast(DecimalType(18, 2))


# =========================================================
# PIPELINE (desde aquí hacia abajo)
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
          F.col("MATORGANICO").alias("MAT1_ESTADOS"),
          F.col("MATORGANICOPASO").alias("MAT2_ESTADOS"),
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
      .withColumn("MAT3_OK", F.when(~es_sup_o_ger(F.col("MAT3_PRODUCTOS")), F.col("MAT3_PRODUCTOS")))
      .withColumn("MAT4_OK", F.when(~es_sup_o_ger(F.col("MAT4_PRODUCTOS")), F.col("MAT4_PRODUCTOS")))
      .withColumn(
          "MATANALISTA_FINAL",
          F.coalesce(F.col("MATANALISTA_ESTADOS"), F.col("MAT3_OK"), F.col("MAT4_OK"))
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
      .select(
          "CODSOLICITUD",
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
      .withColumn("FLGAUTONOMIAOBSERVADA", F.coalesce(F.col("FLGAUTONOMIAOBSERVADA"), F.lit(0)))
)

# 6) SNAPSHOTS DE ESTADOS + PRODUCTOS
df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

# 7) ENSAMBLE FINAL (base = df_last_estado)
df_final = (
    df_last_estado
      .join(df_final_autonomias, on="CODSOLICITUD", how="left")
      .join(df_prod_snap, on="CODSOLICITUD", how="left")
)

# 8) PRODUCTO (TC/CEF) + MATSUPERIOR (desde orgánico usando MATANALISTA_FINAL + mes)
df_final = add_producto_tipo(df_final)
df_final = add_matsuperior_from_organico(df_final, df_org)

# 9) POWERAPPS (fallback SOLO si sigue NULL) + motivos
df_final = apply_powerapps_fallback(df_final, df_apps)

# 9.1) Re-llenar MATSUPERIOR por si PowerApps completó MATANALISTA_FINAL
df_final = add_matsuperior_from_organico(df_final, df_org)

# 9.2) Montos a decimal
df_final = df_final.withColumn("MTOAPROBADO_DEC", to_decimal_monto(F.col("MTOAPROBADO")))

# 9.3) Autonomía por monto SOLO si NO hubo autonomía por PASO (MATAUTONOMIA null)
cond_sin_aut_paso = F.col("MATAUTONOMIA").isNull()

cond_monto_sup = (F.col("MTOAPROBADO_DEC") > F.lit(100000)) & (F.col("MTOAPROBADO_DEC") <= F.lit(240000))
cond_monto_ger = (F.col("MTOAPROBADO_DEC") > F.lit(240000))

mataut_monto = (
    F.when(cond_monto_ger, F.lit("U17293"))
     .when(cond_monto_sup, F.col("MATSUPERIOR"))
)

nivel_monto = (
    F.when(cond_monto_ger, F.lit("GERENTE"))
     .when(cond_monto_sup, F.lit("SUPERVISOR"))
)

df_final = (
    df_final
      .withColumn(
          "MATAUTONOMIA_FINAL",
          F.when(cond_sin_aut_paso, mataut_monto).otherwise(F.col("MATAUTONOMIA"))
      )
      .withColumn(
          "NIVELAUTONOMIA_FINAL",
          F.when(cond_sin_aut_paso, nivel_monto).otherwise(F.col("NIVELAUTONOMIA"))
      )
      .withColumn(
          "AUTONOMIA_REGLA",
          F.when(F.col("MATAUTONOMIA").isNotNull(), F.lit("PASO"))
           .when(cond_sin_aut_paso & mataut_monto.isNotNull(), F.lit("MONTO"))
           .otherwise(F.lit("SIN"))
      )
      # UN SOLO FLAG FINAL
      .withColumn(
          "FLGAUTONOMIA",
          F.when(F.col("MATAUTONOMIA_FINAL").isNotNull(), F.lit(1)).otherwise(F.lit(0))
      )
      # Reemplaza columnas base por finales
      .drop("MATAUTONOMIA", "NIVELAUTONOMIA")
      .withColumnRenamed("MATAUTONOMIA_FINAL", "MATAUTONOMIA")
      .withColumnRenamed("NIVELAUTONOMIA_FINAL", "NIVELAUTONOMIA")
      .drop("MTOAPROBADO_DEC")
)

# 10) SELECT FINAL ORDENADO
df_final = df_final.select(
    "CODSOLICITUD",
    "PRODUCTO",
    "CODMESEVALUACION",

    "TS_BASE_ESTADOS",
    "MATANALISTA_FINAL",
    "ORIGEN_MATANALISTA",
    "MATSUPERIOR",

    "FLGAUTONOMIA",
    "FLGAUTONOMIAOBSERVADA",
    "NIVELAUTONOMIA",
    "MATAUTONOMIA",
    "AUTONOMIA_REGLA",
    "PASO_AUTONOMIA",
    "TS_AUTONOMIA",

    "PROCESO",
    "ESTADOSOLICITUD",
    "ESTADOSOLICITUDPASO",
    "TS_ULTIMO_EVENTO_ESTADOS",
    "FECINICIOEVALUACION_ULT",

    "NBRPRODUCTO",
    "ETAPA",
    "TIPACCION",
    "NBRDIVISA",
    "MTOSOLICITADO",
    "MTOAPROBADO",
    "MTOOFERTADO",
    "MTODESEMBOLSADO",
    "TS_PRODUCTOS",

    "MOTIVORESULTADOANALISTA",
    "MOTIVOMALADERIVACION",
    "SUBMOTIVOMALADERIVACION",
)

# 11) WRITE
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.TP_SOLICITUDES_CENTRALIZADO")
