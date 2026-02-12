from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

BASE_DIR_ORGANICO   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/1n_Activos_*.csv"
PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/BASESOLICITUDES/POWERAPP_EDV.csv"
PATH_SF_ESTADOS     = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"
PATH_SF_PRODUCTOS   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"

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
    Normalización 'robusta':
    - upper
    - quitar tildes
    - normalizar espacios
    - dejar letras/números/espacios (reduce ruido de caracteres especiales)
    """
    c = col.cast("string")
    c = F.upper(c)
    c = quitar_tildes(c)
    c = F.regexp_replace(c, u"\u00A0", " ")
    c = F.regexp_replace(c, u"\u202F", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    # opcional: quitar puntuación “extraña” que rompe tokens (mantiene letras/números/espacio)
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
    
    
def to_decimal_monto(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.trim(s)
    s = F.regexp_replace(s, r"\s+", "")
    # robustez ante separadores, por si aparecen
    s = F.when((F.instr(s, ",") > 0) & (F.instr(s, ".") > 0), F.regexp_replace(s, r"\.", "")).otherwise(s)
    s = F.when(F.instr(s, ",") > 0, F.regexp_replace(s, ",", ".")).otherwise(s)
    return s.cast(DecimalType(18, 2))
    
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

    # Normalización (incluye nombres porque se usan solo transitoriamente para match)
    df = norm_col(df, [
        "ESTADOSOLICITUD", "ESTADOSOLICITUDPASO", "NBRPASO", "NBRULTACTOR", "NBRULTACTORPASO", "PROCESO"
    ])

    # Limpieza (CESADO) también en Salesforce
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
        F.col("Analista").alias("NBRANALISTA"),                    # (MAT3)
        F.col("Analista de crédito").alias("NBRANALISTAASIGNADO"), # (MAT4)
        F.col("Tipo de Acción").alias("TIPACCION"),
        F.col("Fecha de creación").alias("FECCREACION_RAW"),
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
    ).filter(F.col("FLG_CODSOLICITUD_VALIDO") == 1).drop("FLG_CODSOLICITUD_VALIDO")

    # Normaliza textos (incluye nombres solo para match transitorio)
    df = norm_col(df, [
        "NBRPRODUCTO","ETAPA","NBRANALISTA","NBRANALISTAASIGNADO","TIPACCION","NBRDIVISA","CENTROATENCION"
    ])
    df = (
        df.withColumn("NBRANALISTA", limpiar_cesado(F.col("NBRANALISTA")))
          .withColumn("NBRANALISTAASIGNADO", limpiar_cesado(F.col("NBRANALISTAASIGNADO")))
    )

    # Fecha creación (puede venir en distintos formatos)
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

    # Montos a decimal “en bruto”
    df = (
        df.withColumn("MTOSOLICITADO",  to_decimal_monto(F.col("MTOSOLICITADO")))
          .withColumn("MTOAPROBADO",    to_decimal_monto(F.col("MTOAPROBADO")))
          .withColumn("MTOOFERTADO",    to_decimal_monto(F.col("MTOOFERTADO")))
          .withColumn("MTODESEMBOLSADO",to_decimal_monto(F.col("MTODESEMBOLSADO")))
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
    df = norm_col(df, [
        "MATANALISTA", "PRODUCTO", "RESULTADOANALISTA",
        "MOTIVORESULTADOANALISTA", "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"
    ])

    # Timestamp de creación (para quedarnos con el último registro por CODSOLICITUD)
    # Intentamos primero tu parse "esp", si no, dejamos fallback genérico.
    df = (
        df.withColumn("TS_APPS",
            F.coalesce(
                parse_fecha_hora_esp(F.col("FECHORACREACION")),
                F.to_timestamp(F.col("FECHORACREACION"))
            )
        )
    )

    w = Window.partitionBy("CODSOLICITUD").orderBy(F.col("TS_APPS").desc_nulls_last())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")
    return df



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
    bm_analista = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_productos,
        codmes_sf_col="CODMESCREACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRANALISTA",
        min_tokens=3,
        min_ratio_sf=0.60
    )

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
                F.col("MATORGANICO").alias("MATORGANICO_ANALISTA"),  # MAT3
                F.col("MATSUPERIOR").alias("MATSUPERIOR_ANALISTA")
            ),
            on=["CODMESCREACION","CODSOLICITUD"],
            how="left"
        )
        .join(
            bm_asignado.select(
                "CODMESCREACION","CODSOLICITUD",
                F.col("MATORGANICO").alias("MATORGANICO_ASIGNADO"),  # MAT4
                F.col("MATSUPERIOR").alias("MATSUPERIOR_ASIGNADO")
            ),
            on=["CODMESCREACION","CODSOLICITUD"],
            how="left"
        )
    )
    return df_enriq


def build_estado_analista_snapshot(df_estados_enriq):
    """
    Devuelve 1 fila por CODSOLICITUD con:
    - ESTADOSOLICITUDANALISTA: última "decisión" del analista con prioridad:
        1) APROBADO/RECHAZADO (más reciente)
        2) RECUPERADA (más reciente)
        3) PENDIENTE (si nunca hubo APROBADO/RECHAZADO/RECUPERADA)
    - TS_DECISION_ANALISTA: timestamp del evento donde ocurrió la decisión elegida (si existe)
    - TS_ULTIMO_EVENTO_PASO_ANALISTA: timestamp del último evento del paso analista (incluye PENDIENTE)
    """

    # 1) Definir "paso de analista" (misma lógica que atribución)
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

    # 2) Último evento del paso analista (incluye PENDIENTE)
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

    # 3) Última decisión fuerte (APROBADO/RECHAZADO) ignorando PENDIENTE
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

    # 4) Si no hubo fuerte, última decisión alternativa: RECUPERADA
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

    # 5) Ensamble final con prioridad: fuerte > recuperada > pendiente
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
            "MTOOFERTADO",
            "MTODESEMBOLSADO",
            F.col("FECCREACION").alias("TS_PRODUCTOS"),
            # MAT3/MAT4 ya enriquecidos (útiles para atribución)
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
    
    


def add_matsuperior_from_organico(df_final, df_org):
    df_org_key = (
        df_org
        .select(
            F.col("CODMES").cast("string").alias("CODMESEVALUACION"),
            F.col("MATORGANICO").alias("MATANALISTA_FINAL"),
            F.col("MATSUPERIOR").alias("MATSUPERIOR_ORG")
        )
        .dropDuplicates(["CODMESEVALUACION", "MATANALISTA_FINAL"])
    )

    df_out = df_final.join(df_org_key, on=["CODMESEVALUACION", "MATANALISTA_FINAL"], how="left")

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
            F.when(cond_override, F.col("MATANALISTA_APPS"))                       # override
             .otherwise(F.coalesce(F.col("MATANALISTA_FINAL"), F.col("MATANALISTA_APPS")))  # fallback nulos
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
    
    

# 12.1 Staging
df_org       = load_organico(spark, BASE_DIR_ORGANICO)
df_org_tokens= build_org_tokens(df_org)

df_estados   = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos = load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps      = load_powerapps(spark, PATH_PA_SOLICITUDES)

# 12.2 Enriquecimiento con orgánico (matrículas)
df_estados_enriq   = enrich_estados_con_organico(df_estados, df_org_tokens)
df_productos_enriq = enrich_productos_con_organico(df_productos, df_org_tokens)

# NUEVO: snapshot de decisión analista + TS
df_estado_analista = build_estado_analista_snapshot(df_estados_enriq)

# 12.3 Snapshots (1 fila por solicitud)
df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

# 12.3.1 (opcional pero recomendado): adjuntar campos analista al snapshot base
df_last_estado = df_last_estado.join(df_estado_analista, on="CODSOLICITUD", how="left")

# 12.4 Atribución analista final + origen (MAT1/MAT2/MAT3/MAT4)
df_matanalista = build_matanalista_final(df_estados_enriq, df_prod_snap)

# 12.5 Ensamble final (base = último estado)
df_final = (
    df_last_estado
    .join(df_matanalista, on="CODSOLICITUD", how="left")
    .join(df_prod_snap.select(
        "CODSOLICITUD",
        "NBRPRODUCTO","ETAPA","TIPACCION","NBRDIVISA",
        "MTOSOLICITADO","MTOAPROBADO","MTOOFERTADO","MTODESEMBOLSADO",
        "TS_PRODUCTOS"
    ), on="CODSOLICITUD", how="left")
)
# 12.6 Producto (TC/CEF)
df_final = add_producto_tipo(df_final)

# 12.7 MATSUPERIOR por orgánico (mes + matrícula)
df_final = add_matsuperior_from_organico(df_final, df_org)

# 12.8 PowerApps fallback + motivos
df_final = apply_powerapps_fallback(df_final, df_apps)

# 12.9 Re-llenar MATSUPERIOR por si PowerApps completó MATANALISTA_FINAL
df_final = add_matsuperior_from_organico(df_final, df_org)

# 12.10 Selección final (sin autonomías)
df_final = df_final.select(
    "CODSOLICITUD",
    "PRODUCTO",
    "CODMESEVALUACION",

    "TS_BASE_ESTADOS",
    "MATANALISTA_FINAL",
    "ORIGEN_MATANALISTA",
    "MATSUPERIOR",

    "PROCESO",
    "ESTADOSOLICITUD",
    "ESTADOSOLICITUDPASO",
    "TS_ULTIMO_EVENTO_ESTADOS",
    "TS_FIN_ULTIMO_EVENTO_ESTADOS",
    "FECINICIOEVALUACION_ULT",
    "FECFINEVALUACION_ULT",

    "ESTADOSOLICITUDANALISTA",
    "TS_DECISION_ANALISTA",
    "TS_ULTIMO_EVENTO_PASO_ANALISTA",

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





df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.TP_SOLICITUDES_CENTRALIZADO")
