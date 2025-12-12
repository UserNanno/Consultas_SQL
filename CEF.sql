# 🔧 Pipeline de Integración — Orgánico + Salesforce  
**Autor:** (tu nombre)  
**Fecha:** (dd/mm/yyyy)  

Este notebook realiza el procesamiento completo de:
- Orgánico mensual
- Salesforce Estados (workflow)
- Salesforce Productos (oportunidades)
- Enriquecimiento de actores (nombres → matrículas)
- Lógica de roles (analista, supervisor, gerente)
- Flags de autonomía y generación de tabla final.

## 🧩 1. Configuración Inicial  
### Imports, rutas base y funciones utilitarias

Esta sección contiene:
- Helpers reutilizables para limpieza de texto  
- Normalización de tildes  
- Parsers de fecha en español  
- Función genérica de matching basada en nombres tokenizados  

> Esta celda debe ejecutarse primero porque provee funciones para el resto del notebook.


from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re
from functools import reduce

# ---------- PATHS ----------
BASE_DIR_ORGANICO = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/"
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
















## 🧩 2. Carga y Normalización del Orgánico  

### Objetivos:
- Leer archivos Excel del orgánico mensual.
- Normalizar columnas (upper, quitar tildes, trim).
- Estandarizar la estructura.
- Tokenizar nombres para el proceso de matching.
- Preparar un DF (`df_org_tokens`) reutilizable en los matchings.

### Resultado:
Esta sección genera:
- `df_organico` → datos limpios del orgánico  
- `df_org_tokens` → tokens por nombre, usado para emparejar con Salesforce  


entries = dbutils.fs.ls(BASE_DIR_ORGANICO)
files = [e.path for e in entries if e.name.startswith("1n_Activos_2025") and e.name.endswith(".xlsx")]
assert files, "No se encontraron archivos 1n_Activos_2025*.xlsx"

excel_options = {
    "header": "true",
    "inferSchema": "true",
    "treatEmptyValuesAsNulls": "true",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
}

dfs_org = []
for path in files:
    reader = spark.read.format("com.crealytics.spark.excel")
    for k, v in excel_options.items():
        reader = reader.option(k, v)
    df_tmp = reader.load(path)

    for c in df_tmp.columns:
        new_name = re.sub(r"\s+", " ", c.strip())
        if new_name != c:
            df_tmp = df_tmp.withColumnRenamed(c, new_name)

    cols_clean = [c for c in df_tmp.columns if not re.match(r"^_c\d+$", c)]
    df_tmp = df_tmp.select(*cols_clean)

    dfs_org.append(df_tmp)

df_organico_raw = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_org)

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






















## 🧩 3. Salesforce — Estados (Histórico de pasos del flujo)

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
        )
)

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("ESTADOSOLICITUD", norm_txt_spark("ESTADOSOLICITUD"))
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



























## 🧩 4. Matching de Actores en Estados

### Objetivos:
- Transformar nombres de Salesforce en tokens.
- Comparar tokens con los tokens del Orgánico por mes.
- Calcular:
  - `TOKENS_MATCH`
  - `RATIO_SF`
  - `RATIO_ORG`
- Aplicar tolerancia mínima (3 tokens coincidentes y 60% de ratio).
- Determinar:
  - Matrícula del actor (`MATORGANICO`)
  - Matrícula del actor del paso (`MATORGANICOPASO`)

### Resultado:
Se genera `df_salesforce_enriq` con matrículas pegadas.


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




























## 🧩 5. Salesforce — Productos (Oportunidades)

### Objetivos:
- Cargar informe de oportunidades.
- Normalizar texto (upper + quitar tildes).
- Parseo robusto de fechas de creación.
- Generar columna estándar `CODMESCREACION`.

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
            F.col("Fecha de creación").alias("FECCREACION")
        )
)

df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("NBRPRODUCTO", norm_txt_spark("NBRPRODUCTO"))
        .withColumn("ETAPA", norm_txt_spark("ETAPA"))
        .withColumn("NBRANALISTA", norm_txt_spark("NBRANALISTA"))
        .withColumn("TIPACCION", norm_txt_spark("TIPACCION"))
)

df_salesforce_productos = (
    df_salesforce_productos
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

df_salesforce_productos = df_salesforce_productos.withColumn(
    "CODMESCREACION", F.col("CODMESCREACION").cast("string")
)

print("Registros originales SF productos:", df_salesforce_productos.count())





























## 🧩 6. Matching del Analista de la Oportunidad

### Objetivos:
- Tokenizar el nombre del analista en productos.
- Compararlo contra el Orgánico del mes correspondiente.
- Identificar:
  - `MATORGANICO_ANALISTA`
  - `MATSUPERIOR_ANALISTA`

### Resultado:
`df_productos_enriq` enriquecido con matrículas del analista.


df_best_match_analista = match_persona_vs_organico(
    df_org_tokens=df_org_tokens,
    df_sf=df_salesforce_productos,
    codmes_sf_col="CODMESCREACION",
    codsol_col="CODSOLICITUD",
    nombre_sf_col="NBRANALISTA",
    min_tokens=3,
    min_ratio_sf=0.60
)

df_productos_enriq = (
    df_salesforce_productos
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








## 🧩 10. Persistencia en Catálogo (Managed Table)

### Instrucciones:
La tabla final se persiste como tabla "managed" sin necesidad de path físico:

```sql
CREATE TABLE catalog.schema.nombre_tabla
AS SELECT * FROM df_solicitudes_final
