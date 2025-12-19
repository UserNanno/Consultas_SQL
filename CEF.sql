%md
# Pipeline de Integración — Orgánico + Salesforce  
**Autor:** Mariano Canecillas 

Este notebook realiza el procesamiento completo de:
- Orgánico mensual
- Salesforce Estados (workflow)
- Salesforce Productos (oportunidades)
- Enriquecimiento de actores (nombres → matrículas)
- Lógica de roles (analista, supervisor, gerente)
- Flags de autonomía y generación de tabla final


%md
## 1. Configuración Inicial  
### Imports, rutas base y funciones utilitarias

Esta sección contiene:
- Helpers reutilizables para limpieza de texto  
- Normalización de tildes  
- Parsers de fecha en español  
- Función genérica de matching basada en nombres tokenizados  

> Esta celda debe ejecutarse primero porque provee funciones para el resto del notebook



from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re
from functools import reduce

# ---------- PATHS ----------
BASE_DIR_ORGANICO = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/1n_Activos_*.csv"
PATH_SF_ESTADOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"
PATH_SF_PRODUCTOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"

# ---------- HELPERS GENERICOS ----------

def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def norm_txt_spark(col_name):
    return F.trim(quitar_tildes(F.upper(F.col(col_name))))

def limpiar_cesado(col_name):
    return F.trim(F.regexp_replace(F.col(col_name), r'\(CESADO\)', ''))

def parse_fecha_hora_esp_col(col_name):
    s = F.col(col_name).cast("string")
    s = F.regexp_replace(s, u'\u00A0', ' ')
    s = F.regexp_replace(s, u'\u202F', ' ')
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r'\s+', ' ')
    s = F.regexp_replace(s, r'(?i)a\W*m\W*', 'AM')
    s = F.regexp_replace(s, r'(?i)p\W*m\W*', 'PM')
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')

def match_persona_vs_organico(
    df_org_tokens,
    df_sf,
    codmes_sf_col,
    codsol_col,
    nombre_sf_col,
    min_tokens=3,
    min_ratio_sf=0.60
):
    # Tokenizar nombre en SF
    df_sf_nombres = (
        df_sf
          .withColumn(
              "TOKENS_NOMBRE_SF",
              F.array_distinct(F.split(F.col(nombre_sf_col), r"\s+"))
          )
          .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
    )

    df_sf_tokens = (
        df_sf_nombres
          .select(
              F.col(codmes_sf_col),
              F.col(codsol_col),
              F.col(nombre_sf_col),
              F.col("TOKENS_NOMBRE_SF"),
              F.col("N_TOK_SF")
          )
          .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF"))
    )

    # Join por MES + TOKEN
    df_join_tokens = (
        df_sf_tokens.alias("sf")
          .join(
              df_org_tokens.alias("org"),
              (F.col(f"sf.{codmes_sf_col}") == F.col("org.CODMES")) &
              (F.col("sf.TOKEN") == F.col("org.TOKEN")),
              "inner"
          )
    )

    match_cols = [
        codmes_sf_col,
        codsol_col,
        nombre_sf_col,
        "MATORGANICO",
        "MATSUPERIOR"
    ]

    df_match_scores = (
        df_join_tokens
          .groupBy(match_cols)
          .agg(
              F.countDistinct("sf.TOKEN").alias("TOKENS_MATCH"),
              F.first("sf.N_TOK_SF").alias("N_TOK_SF"),
              F.first("org.N_TOK_ORG").alias("N_TOK_ORG"),
          )
          .withColumn("RATIO_SF", F.col("TOKENS_MATCH") / F.col("N_TOK_SF"))
          .withColumn("RATIO_ORG", F.col("TOKENS_MATCH") / F.col("N_TOK_ORG"))
    )

    df_match_filtrado = (
        df_match_scores
          .filter(
              (F.col("TOKENS_MATCH") >= min_tokens) &
              (F.col("RATIO_SF") >= min_ratio_sf)
          )
    )

    w = Window.partitionBy(codmes_sf_col, codsol_col).orderBy(
        F.col("TOKENS_MATCH").desc(),
        F.col("RATIO_SF").desc(),
        F.col("RATIO_ORG").desc(),
        F.col("MATORGANICO").asc()
    )

    df_best_match = (
        df_match_filtrado
          .withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )

    return df_best_match




%md
## 2. Carga y Normalización del Orgánico  

### Objetivos:
- Leer archivos Excel del orgánico mensual
- Normalizar columnas (upper, quitar tildes, trim)
- Estandarizar la estructura
- Tokenizar nombres para el proceso de matching
- Preparar un DF (`df_org_tokens`) reutilizable en los matchings

### Resultado:
Esta sección genera:
- `df_organico` → datos limpios del orgánico 
- `df_org_tokens` → tokens por nombre, usado para emparejar con Salesforce 



df_organico_raw = (
   spark.read
       .format("csv")
       .option("header", "true")
       .option("sep", ";")
       .option("encoding", "ISO-8859-1")
       .option("ignoreLeadingWhiteSpace", "true")
       .option("ignoreTrailingWhiteSpace", "true")
       .load(BASE_DIR_ORGANICO)
)

df_organico = (
    df_organico_raw
        .select(
            F.col("CODMES").alias("CODMES"),
            F.col("Matrícula").alias("MATORGANICO"),
            F.col("Nombre Completo").alias("NOMBRECOMPLETO"),
            F.col("Correo electronico").alias("CORREO"),
            F.col("Fecha Ingreso").alias("FECINGRESO"),
            F.col("Matrícula Superior").alias("MATSUPERIOR"),
        )
)

for c in ["MATORGANICO", "NOMBRECOMPLETO", "MATSUPERIOR"]:
    df_organico = df_organico.withColumn(c, norm_txt_spark(c))

df_organico = df_organico.withColumn(
    "MATSUPERIOR",
    F.regexp_replace(F.col("MATSUPERIOR"), r'^0(?=[A-Z]\d{5})', '')
)

df_organico = df_organico.withColumn("FECINGRESO", F.to_date("FECINGRESO"))
df_organico = df_organico.withColumn("NOMBRECOMPLETO_CLEAN", limpiar_cesado("NOMBRECOMPLETO"))
df_organico = df_organico.withColumn("CODMES", F.col("CODMES").cast("string"))

# Tokenización orgánico (para todos los matchings)
df_org_nombres = (
    df_organico
      .withColumn(
          "TOKENS_NOMBRE_ORG",
          F.array_distinct(F.split(F.col("NOMBRECOMPLETO"), r"\s+"))
      )
      .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
)

df_org_tokens = (
    df_org_nombres
      .select(
          "CODMES",
          "MATORGANICO",
          "MATSUPERIOR",
          "NOMBRECOMPLETO",
          "TOKENS_NOMBRE_ORG",
          "N_TOK_ORG"
      )
      .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_ORG"))
)

print("Organico listo:", df_organico.count())



%md
## 3. Salesforce — Estados (Histórico de pasos del flujo)

### Objetivos:
- Cargar el informe "INFORME_ESTADO" desde Azure.
- Normalizar texto: mayúsculas, quitar tildes, limpiar espacios.
- Parsear fechas con formato latino ("a. m.", "p. m.").
- Generar columnas estándar:
  - `FECINICIOEVALUACION`
  - `FECHORINICIOEVALUACION`
  - `HORINICIOEVALUACION`
  - `CODMESEVALUACION` (YYYYMM)

### Resultado:
Se obtiene `df_salesforce_estados` limpio y listo para matching.



df_sf_estados_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(PATH_SF_ESTADOS)
)

df_salesforce_estados = (
    df_sf_estados_raw
        .select(
            F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION"),
            F.col("Fecha de finalización del paso").alias("FECFINEVALUACION"),
            F.col("Nombre del registro").alias("CODSOLICITUD"),
            F.col("Estado").alias("ESTADOSOLICITUD"),
            F.col("Paso: Nombre").alias("NBRPASO"),
            F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
            F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
            F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
            F.col("Estado del paso").alias("ESTADOSOLICITUDPASO")
        )
)

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("ESTADOSOLICITUD", norm_txt_spark("ESTADOSOLICITUD"))
        .withColumn("ESTADOSOLICITUDPASO", norm_txt_spark("ESTADOSOLICITUDPASO"))
        .withColumn("NBRPASO", norm_txt_spark("NBRPASO"))
        .withColumn("NBRULTACTOR", norm_txt_spark("NBRULTACTOR"))
        .withColumn("NBRULTACTORPASO", norm_txt_spark("NBRULTACTORPASO"))
        .withColumn("PROCESO", norm_txt_spark("PROCESO"))
)

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp_col("FECINICIOEVALUACION"))
        .withColumn("FECHORFINEVALUACION", parse_fecha_hora_esp_col("FECFINEVALUACION"))
        .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
        .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))
        .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
        .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))
        .withColumn("CODMESEVALUACION", F.date_format("FECINICIOEVALUACION", "yyyyMM"))
)

df_salesforce_estados = df_salesforce_estados.withColumn(
    "CODMESEVALUACION", F.col("CODMESEVALUACION").cast("string")
)

print("Registros originales SF estados:", df_salesforce_estados.count())



%md
## 4. Matching de Actores en Estados

### Objetivos:
- Transformar nombres de Salesforce en tokens
- Comparar tokens con los tokens del Orgánico por mes
- Calcular:
  - `TOKENS_MATCH`
  - `RATIO_SF`
  - `RATIO_ORG`
- Aplicar tolerancia mínima (3 tokens coincidentes y 60% de ratio)
- Determinar:
  - Matrícula del actor (`MATORGANICO`)
  - Matrícula del actor del paso (`MATORGANICOPASO`)

### Resultado:
Se genera `df_salesforce_enriq` con matrículas pegadas



# MATCH 1: NBRULTACTOR
df_best_match_ultactor = match_persona_vs_organico(
    df_org_tokens=df_org_tokens,
    df_sf=df_salesforce_estados,
    codmes_sf_col="CODMESEVALUACION",
    codsol_col="CODSOLICITUD",
    nombre_sf_col="NBRULTACTOR",
    min_tokens=3,
    min_ratio_sf=0.60
)

# MATCH 2: NBRULTACTORPASO
df_best_match_paso = match_persona_vs_organico(
    df_org_tokens=df_org_tokens,
    df_sf=df_salesforce_estados,
    codmes_sf_col="CODMESEVALUACION",
    codsol_col="CODSOLICITUD",
    nombre_sf_col="NBRULTACTORPASO",
    min_tokens=3,
    min_ratio_sf=0.60
)

df_salesforce_enriq = (
    df_salesforce_estados
      .join(
          df_best_match_ultactor.select(
              "CODMESEVALUACION",
              "CODSOLICITUD",
              "MATORGANICO",
              "MATSUPERIOR"
          ),
          on=["CODMESEVALUACION", "CODSOLICITUD"],
          how="left"
      )
      .join(
          df_best_match_paso.select(
              "CODMESEVALUACION",
              "CODSOLICITUD",
              F.col("MATORGANICO").alias("MATORGANICOPASO"),
              F.col("MATSUPERIOR").alias("MATSUPERIORPASO")
          ),
          on=["CODMESEVALUACION", "CODSOLICITUD"],
          how="left"
      )
)

print("Registros enriquecidos estados:", df_salesforce_enriq.count())



%md
## 5. Salesforce — Productos

### Objetivos:
- Cargar informe de solicitudes donde figura el producto, etapa
- Normalizar texto (upper + quitar tildes)
- Parseo robusto de fechas de creación
- Generar columna estándar `CODMESCREACION`

### Resultado:
`df_salesforce_productos` limpio y preparado para matching del analista.





df_sf_productos_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(PATH_SF_PRODUCTOS)
)

df_salesforce_productos = (
    df_sf_productos_raw
        .select(
            F.col("Nombre de la oportunidad").alias("CODSOLICITUD"),
            F.col("Nombre del Producto").alias("NBRPRODUCTO"),
            F.col("Etapa").alias("ETAPA"),
            F.col("Analista de crédito").alias("NBRANALISTA"),
            F.col("Tipo de Acción").alias("TIPACCION"),
            F.col("Fecha de creación").alias("FECCREACION"),
            F.col("Divisa de la oportunidad").alias("NBRDIVISA"),
            F.col("Monto/Línea Solicitud").alias("MTOSOLICITADO"),
            F.col("Monto/Línea aprobada").alias("MTOAPROBADO"),
            F.col("Monto Solicitado/Ofertado").alias("MTOOFERTADO"),
            F.col("Monto desembolsado").alias("MTODESEMBOLSADO"),
            F.col("Centralizado/Punto de Contacto").alias("CENTROATENCION")
        )
)

df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("CODSOLICITUD_CLEAN", F.trim(F.col("CODSOLICITUD").cast("string")))
        .withColumn(
            "FLG_CODSOLICITUD_VALIDO",
            F.when(
                (F.length("CODSOLICITUD_CLEAN") == 11) &
                (F.col("CODSOLICITUD_CLEAN").startswith("O00")),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
)

df_salesforce_productos_validos = df_salesforce_productos.filter(F.col("FLG_CODSOLICITUD_VALIDO") == 1)
df_salesforce_productos_invalidos = df_salesforce_productos.filter(F.col("FLG_CODSOLICITUD_VALIDO") == 0)

print("Productos validos:", df_salesforce_productos_validos.count())
print("Productos invalidos:", df_salesforce_productos_invalidos.count())

# Quedarnos con CODSOLICITUD limpio:
df_salesforce_productos_validos = (
    df_salesforce_productos_validos
        .drop("CODSOLICITUD")
        .withColumnRenamed("CODSOLICITUD_CLEAN", "CODSOLICITUD")
)

# Normalización texto y parseo de fecha y CODMESCREACION (solo en válidos)
df_salesforce_productos_validos = (
    df_salesforce_productos_validos
        .withColumn("NBRPRODUCTO", norm_txt_spark("NBRPRODUCTO"))
        .withColumn("ETAPA", norm_txt_spark("ETAPA"))
        .withColumn("NBRANALISTA", norm_txt_spark("NBRANALISTA"))
        .withColumn("TIPACCION", norm_txt_spark("TIPACCION"))
        .withColumn("NBRMONEDASOLICITADA", norm_txt_spark("NBRDIVISA"))
        .withColumn("CENTROATENCION", norm_txt_spark("CENTROATENCION"))
)

df_salesforce_productos_validos = (
    df_salesforce_productos_validos
        .withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION").cast("string")))
        .withColumn(
            "FECCREACION_DATE",
            F.coalesce(
                F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                F.to_date("FECCREACION_STR")
            )
        )
        .withColumn("CODMESCREACION", F.date_format("FECCREACION_DATE", "yyyyMM"))
        .withColumn("FECCREACION", F.col("FECCREACION_DATE"))
        .drop("FECCREACION_STR", "FECCREACION_DATE")
)

df_salesforce_productos_validos = df_salesforce_productos_validos.withColumn(
    "CODMESCREACION", F.col("CODMESCREACION").cast("string")
)

print("Registros SF productos (validos):", df_salesforce_productos_validos.count())



from pyspark.sql import functions as F

df = df_salesforce_productos_validos

total_rows = df.count()
distinct_rows = df.distinct().count()
duplicated_rows_excess = total_rows - distinct_rows

print("Total de filas:", total_rows)
print("Filas distintas:", distinct_rows)
print("Filas repetidas (exceso):", duplicated_rows_excess)





codsol_duplicated_groups = (
    df.groupBy("CODSOLICITUD")
      .agg(F.count(F.lit(1)).alias("n"))
      .filter(F.col("n") > 1)
)

num_codsol_duplicated_groups = codsol_duplicated_groups.count()
print("Número de CODSOLICITUD distintos con duplicados:", num_codsol_duplicated_groups)



%md
## 6. Matching del Analista de la Oportunidad

### Objetivos:
- Tokenizar el nombre del analista en productos
- Compararlo contra el Orgánico del mes correspondiente
- Identificar:
  - `MATORGANICO_ANALISTA`
  - `MATSUPERIOR_ANALISTA`

### Resultado:
`df_productos_enriq` enriquecido con matrículas del analista



df_best_match_analista = match_persona_vs_organico(
    df_org_tokens=df_org_tokens,
    df_sf=df_salesforce_productos_validos,
    codmes_sf_col="CODMESCREACION",
    codsol_col="CODSOLICITUD",
    nombre_sf_col="NBRANALISTA",
    min_tokens=3,
    min_ratio_sf=0.60
)

df_productos_enriq = (
    df_salesforce_productos_validos
      .join(
          df_best_match_analista.select(
              "CODMESCREACION",
              "CODSOLICITUD",
              F.col("MATORGANICO").alias("MATORGANICO_ANALISTA"),
              F.col("MATSUPERIOR").alias("MATSUPERIOR_ANALISTA")
          ),
          on=["CODMESCREACION", "CODSOLICITUD"],
          how="left"
      )
)

print("Registros enriquecidos productos:", df_productos_enriq.count())


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# PARAMETROS: HARD CODE
# =========================
gerentes = ["U17293"]
supervisores = ["U17560", "U13421", "S18795", "U18900", "E12624", "U23436"]

def rol_por_matricula(col_mat):
    return (
        F.when(col_mat.isin(gerentes), F.lit("GERENTE"))
         .when(col_mat.isin(supervisores), F.lit("SUPERVISOR"))
         .when(col_mat.isNotNull(), F.lit("ANALISTA"))
         .otherwise(F.lit(None))
    )

def es_sup_o_ger(col_mat):
    return col_mat.isin(gerentes + supervisores)




# =========================================================
# 1) Productos base (Mat3) - sin nombres, 1 fila por solicitud
# =========================================================
df_productos_base = (
    df_productos_enriq
      .select(
          "CODSOLICITUD",
          "NBRPRODUCTO", "ETAPA", "TIPACCION",
          "NBRDIVISA", "MTOSOLICITADO", "MTOAPROBADO", 
          "MTOOFERTADO", "MTODESEMBOLSADO",
          "CENTROATENCION", "FECCREACION", 
          "CODMESCREACION", "MATORGANICO_ANALISTA",      # Mat3
          "MATSUPERIOR_ANALISTA"
      )
)

# =========================================================
# 2) Clasificación proceso TC / CEF
#    (PROCESO ya viene en MAYUS y sin tildes)
# =========================================================
is_tc = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
is_cef = F.col("PROCESO").isin(
    "CO SOLICITUD APROBACIONES TLMK",
    "SFCP APROBACIONES EDUCATIVO",
    "CO SOLICITUD APROBACIONES"
)

# Pasos
paso_tc_analista   = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
paso_tc_supervisor = (F.col("NBRPASO") == "APROBACION DE CREDITOS SUPERVISOR")
paso_tc_gerente    = (F.col("NBRPASO") == "APROBACION DE CREDITOS GERENTE")

paso_cef_analista  = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")
paso_cef_aprobador = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD APROBADOR")

# =========================================================
# 3) df_last_estado: snapshot MAS ACTUAL por CODSOLICITUD
# =========================================================
w_last = Window.partitionBy("CODSOLICITUD").orderBy(
    F.col("FECHORINICIOEVALUACION").desc(),
    F.col("FECHORFINEVALUACION").desc()
)

df_last_estado = (
    df_salesforce_enriq
      .withColumn("rn_last", F.row_number().over(w_last))
      .filter(F.col("rn_last") == 1)
      .withColumn("TIPO_PRODUCTO", F.when(is_tc, "TC").when(is_cef, "CEF").otherwise("OTRO"))
      .select(
          "CODSOLICITUD",
          "TIPO_PRODUCTO",
          F.col("CODMESEVALUACION").alias("CODMESEVALUACION_ULTIMO"),
          F.col("PROCESO").alias("PROCESO_ULTIMO"),
          F.col("NBRPASO").alias("NBRPASO_ULTIMO"),
          F.col("ESTADOSOLICITUD").alias("ESTADOSOLICITUD_ULTIMO"),
          F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUD_PASO"),
          F.col("FECHORINICIOEVALUACION").alias("FECHORINICIOEVALUACION_ULTIMO"),
          F.col("FECINICIOEVALUACION").alias("FECINICIOEVALUACION_ULTIMO"),
          F.col("HORINICIOEVALUACION").alias("HORINICIOEVALUACION_ULTIMO"),
          F.col("FECHORFINEVALUACION").alias("FECHORFINEVALUACION_ULTIMO"),
          F.col("FECFINEVALUACION").alias("FECFINEVALUACION_ULTIMO"),
          F.col("HORFINEVALUACION").alias("HORFINEVALUACION_ULTIMO"),
      )
)

# =========================================================
# 4) df_analista_from_estados: PASO BASE (Mat1->Mat2), evita sup/ger
#    - TC: APROBACION ... ANALISTA
#    - CEF: EVALUACION DE SOLICITUD
# =========================================================
es_paso_base = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

df_base_candidates = (
    df_salesforce_enriq
      .filter(es_paso_base)
      .withColumn("MAT1", F.col("MATORGANICO"))
      .withColumn("MAT2", F.col("MATORGANICOPASO"))
)

w_base = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORINICIOEVALUACION").desc())

df_base_latest = (
    df_base_candidates
      .withColumn("rn_base", F.row_number().over(w_base))
      .filter(F.col("rn_base") == 1)
      .select("CODSOLICITUD", "MAT1", "MAT2")
      .withColumn("FLG_EXISTE_PASO_BASE", F.lit(1))
)

df_analista_from_estados = (
    df_base_latest
      .withColumn(
          "MAT_ANALISTA_ESTADOS",
          F.when(F.col("MAT1").isNotNull() & (~es_sup_o_ger(F.col("MAT1"))), F.col("MAT1"))
           .when(F.col("MAT2").isNotNull() & (~es_sup_o_ger(F.col("MAT2"))), F.col("MAT2"))
           .otherwise(F.lit(None).cast("string"))
      )
      .withColumn(
          "ORIGEN_MAT_ANALISTA_ESTADOS",
          F.when(F.col("MAT1").isNotNull() & (~es_sup_o_ger(F.col("MAT1"))), F.lit("ESTADOS_MAT1"))
           .when(F.col("MAT2").isNotNull() & (~es_sup_o_ger(F.col("MAT2"))), F.lit("ESTADOS_MAT2"))
           .otherwise(F.lit(None))
      )
      .select(
          "CODSOLICITUD",
          "FLG_EXISTE_PASO_BASE",
          "MAT_ANALISTA_ESTADOS",
          "ORIGEN_MAT_ANALISTA_ESTADOS"
      )
)

# =========================================================
# 5) df_autonomia: autonomía desde estados COMPLETO (sin dedup)
#    - TC: GERENTE/SUPERVISOR por nombre de paso
#    - CEF: APROBADOR, rol por matrícula
# =========================================================

# TC
df_aut_tc = (
    df_salesforce_enriq
      .filter(is_tc & (paso_tc_gerente | paso_tc_supervisor))
      .withColumn("MAT1", F.col("MATORGANICO"))
      .withColumn("MAT2", F.col("MATORGANICOPASO"))
      .withColumn("MAT_AUTONOMIA", F.coalesce(F.col("MAT1"), F.col("MAT2")))
      .withColumn(
          "ROL_AUTONOMIA",
          F.when(paso_tc_gerente, F.lit("GERENTE"))
           .when(paso_tc_supervisor, F.lit("SUPERVISOR"))
      )
      .withColumn(
          "PRIORIDAD_AUTONOMIA",
          F.when(F.col("ROL_AUTONOMIA") == "GERENTE", F.lit(3))
           .when(F.col("ROL_AUTONOMIA") == "SUPERVISOR", F.lit(2))
           .otherwise(F.lit(0))
      )
      .withColumn("NBRPASO_AUTONOMIA", F.col("NBRPASO"))
)

# CEF
df_aut_cef = (
    df_salesforce_enriq
      .filter(is_cef & paso_cef_aprobador)
      .withColumn("MAT1", F.col("MATORGANICO"))
      .withColumn("MAT2", F.col("MATORGANICOPASO"))
      .withColumn("MAT_AUTONOMIA", F.coalesce(F.col("MAT1"), F.col("MAT2")))
      .withColumn("ROL_AUTONOMIA", rol_por_matricula(F.col("MAT_AUTONOMIA")))
      .withColumn(
          "PRIORIDAD_AUTONOMIA",
          F.when(F.col("ROL_AUTONOMIA") == "GERENTE", F.lit(3))
           .when(F.col("ROL_AUTONOMIA") == "SUPERVISOR", F.lit(2))
           .when(F.col("ROL_AUTONOMIA") == "ANALISTA", F.lit(1))
           .otherwise(F.lit(0))
      )
      .withColumn("NBRPASO_AUTONOMIA", F.col("NBRPASO"))
)

df_aut_all = df_aut_tc.unionByName(df_aut_cef, allowMissingColumns=True)

w_aut = Window.partitionBy("CODSOLICITUD").orderBy(
    F.col("PRIORIDAD_AUTONOMIA").desc(),
    F.col("FECHORINICIOEVALUACION").desc()
)

df_autonomia = (
    df_aut_all
      .withColumn("rn_aut", F.row_number().over(w_aut))
      .filter(F.col("rn_aut") == 1)
      .select(
          "CODSOLICITUD",
          "ROL_AUTONOMIA",
          "MAT_AUTONOMIA",
          "NBRPASO_AUTONOMIA",
          F.when(F.col("ROL_AUTONOMIA") == "GERENTE", 1).otherwise(0).alias("FLG_AUTONOMIA_GERENTE"),
          F.when(F.col("ROL_AUTONOMIA") == "SUPERVISOR", 1).otherwise(0).alias("FLG_AUTONOMIA_SUPERVISOR"),
          F.when(F.col("ROL_AUTONOMIA") == "ANALISTA", 1).otherwise(0).alias("FLG_AUTONOMIA_ANALISTA"),
          F.col("FECHORINICIOEVALUACION").alias("TS_AUTONOMIA")
      )
)




df_final_solicitud = (
    df_last_estado
      .join(df_productos_base, on="CODSOLICITUD", how="left")
      .join(df_analista_from_estados, on="CODSOLICITUD", how="left")
      .join(df_autonomia, on="CODSOLICITUD", how="left")
      .withColumn("FLG_FALTA_PASO_BASE", F.when(F.col("FLG_EXISTE_PASO_BASE").isNull(), 1).otherwise(0))
      .withColumn(
          "FLG_AUTONOMIA_SIN_PASO_BASE",
          F.when((F.col("ROL_AUTONOMIA").isNotNull()) & (F.col("FLG_EXISTE_PASO_BASE").isNull()), 1).otherwise(0)
      )
      # Analista final: Mat1/Mat2 (estados) y fallback a Mat3 (productos)
      .withColumn(
          "MAT_ANALISTA_FINAL",
          F.coalesce(F.col("MAT_ANALISTA_ESTADOS"), F.col("MATORGANICO_ANALISTA"))
      )
      .withColumn(
          "ORIGEN_MAT_ANALISTA",
          F.when(F.col("MAT_ANALISTA_ESTADOS").isNotNull(), F.col("ORIGEN_MAT_ANALISTA_ESTADOS"))
           .when(F.col("MATORGANICO_ANALISTA").isNotNull(), F.lit("PRODUCTOS_MAT3"))
           .otherwise(F.lit(None))
      )
      .withColumn("TS_ULTIMO_EVENTO", F.col("FECHORINICIOEVALUACION_ULTIMO"))
)

# Selección final SIN NOMBRES (todo por matrícula)
df_final_solicitud = df_final_solicitud.select(
    "CODSOLICITUD",
    "TIPO_PRODUCTO",

    # Productos
    "CODMESCREACION", "FECCREACION",
    "NBRPRODUCTO", "ETAPA", "TIPACCION",
    "NBRDIVISA", "MTOSOLICITADO", "MTOAPROBADO",
    "MTOOFERTADO", "MTODESEMBOLSADO", "CENTROATENCION",
    "MATORGANICO_ANALISTA", "MATSUPERIOR_ANALISTA",

    # Último estado (snapshot)
    "CODMESEVALUACION_ULTIMO", "PROCESO_ULTIMO",
    "NBRPASO_ULTIMO", "ESTADOSOLICITUD_ULTIMO", "ESTADOSOLICITUD_PASO",
    "FECHORINICIOEVALUACION_ULTIMO", "FECINICIOEVALUACION_ULTIMO", "HORINICIOEVALUACION_ULTIMO",
    "FECHORFINEVALUACION_ULTIMO", "FECFINEVALUACION_ULTIMO", "HORFINEVALUACION_ULTIMO",

    # Analista final
    "MAT_ANALISTA_FINAL", "ORIGEN_MAT_ANALISTA",
    "FLG_FALTA_PASO_BASE",

    # Autonomía
    "ROL_AUTONOMIA", "MAT_AUTONOMIA", "NBRPASO_AUTONOMIA",
    "FLG_AUTONOMIA_GERENTE", "FLG_AUTONOMIA_SUPERVISOR", "FLG_AUTONOMIA_ANALISTA",
    "FLG_AUTONOMIA_SIN_PASO_BASE",
    "TS_AUTONOMIA",

    # Control
    "TS_ULTIMO_EVENTO"
)


# =========================
# VALIDACIONES CLAVE
# =========================
print("Final (1 fila por CODSOLICITUD):", df_final_solicitud.count())


# 1) Confirmar 1 fila por solicitud
dups_final = df_final_solicitud.groupBy("CODSOLICITUD").count().filter("count > 1").count()
print("Duplicados en final (debe ser 0):", dups_final)



# 2) Benchmark de solicitudes (debe coincidir)
n_solic_estados = df_salesforce_enriq.select("CODSOLICITUD").distinct().count()
print("Solicitudes unicas en estados:", n_solic_estados)



%md
## 10. Persistencia en Catálogo (Managed Table)

### Instrucciones:
La tabla final se persiste como tabla "managed" sin necesidad de path físico

df_final_solicitud.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.TP_SOLICITUDES_CENTRALIZADO")
