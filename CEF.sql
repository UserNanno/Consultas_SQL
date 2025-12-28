# Databricks / PySpark notebook
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

%md
## Objetivo del pipeline  
### Consolidación de solicitudes de crédito (última foto)

Este pipeline construye una tabla consolidada con granularidad **1 fila por CODSOLICITUD**, que representa la **última foto del estado de cada solicitud de crédito**, orientada a tableros de seguimiento operativo y desempeño.

**Fuente base (universo):**
- `INFORMES_ESTADOS_*` (Salesforce), ya que contiene el tracking real del flujo de evaluación por pasos.

**Regla de oro:**
- El registro con la **mayor FECHORINICIOEVALUACION (timestamp)** determina el estado vigente de la solicitud:
  - PROCESO  
  - ESTADOSOLICITUD  
  - ESTADOSOLICITUDPASO  

**Fuentes de enriquecimiento:**
- `INFORME_PRODUCTO_*`: atributos de producto y montos (1 registro por solicitud).
- `POWERAPP_EDV.csv`: fuente complementaria usada como **COALESCE** y para motivos.

**Restricción de datos (DAC):**
- No se persisten nombres de personas.
- Los nombres se utilizan únicamente de forma transitoria para mapear contra el orgánico y obtener **matrículas**.



%md
## 0. Configuración de rutas  
### Definición de paths de entrada

En esta sección se definen las rutas hacia todas las fuentes utilizadas en el pipeline:

- Orgánico mensual de colaboradores (`1n_Activos_*`)
- Salesforce Estados (`INFORME_ESTADO_*`)
- Salesforce Productos (`INFORME_PRODUCTO_*`)
- PowerApps EDV

Centralizar los paths permite facilitar mantenimiento, versionamiento y reutilización del notebook.


  
BASE_DIR_ORGANICO   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/1n_Activos_*.csv"
PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/BASESOLICITUDES/POWERAPP_EDV.csv"
PATH_SF_ESTADOS     = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"
PATH_SF_PRODUCTOS   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"


%md
## 1. Normalización de texto  
### Limpieza y estandarización de campos textuales

Se definen funciones utilitarias para estandarizar texto proveniente de múltiples fuentes con inconsistencias de formato.

La normalización incluye:
- Conversión a mayúsculas.
- Eliminación de tildes.
- Normalización de espacios.
- Eliminación del sufijo **(CESADO)**.
- Limpieza de caracteres no alfanuméricos.

Esta estandarización es clave para:
- Reducir falsos negativos en el matching por nombres.
- Asegurar consistencia entre Salesforce, Orgánico y PowerApps.

  
def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def limpiar_cesado(col):
    # quita (CESADO) y dobles espacios
    c = F.regexp_replace(col, r"\(CESADO\)", "")
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


%md
## 2. Parseo de fechas y horas  
### Conversión a timestamp real

Salesforce y PowerApps exportan fechas en formato texto con variaciones regionales y caracteres especiales.

En esta sección se implementa un parser robusto que:
- Limpia caracteres invisibles.
- Normaliza indicadores AM / PM.
- Convierte los valores a tipo **TIMESTAMP**.

El uso de timestamps es fundamental para:
- Ordenar correctamente los eventos del flujo.
- Determinar de forma consistente la **última foto** de cada solicitud.

  
def parse_fecha_hora_esp(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r"\s+", " ")
    s = F.regexp_replace(s, r"(?i)a\W*m\W*", "AM")
    s = F.regexp_replace(s, r"(?i)p\W*m\W*", "PM")
    return F.to_timestamp(s, "dd/MM/yyyy hh:mm a")


%md
## 3. Matching de personas contra Orgánico  
### Tokenización y scoring por coincidencia

Para evitar persistir datos DAC, se implementa un matching tolerante basado en tokens para mapear nombres a matrículas.

Principios del matching:
- Tokenización de nombres (split por espacios).
- Restricción por **mes (CODMES)** para manejar cambios de matrícula.
- Cálculo de métricas de coincidencia:
  - TOKENS_MATCH  
  - RATIO_SF  
  - RATIO_ORG  

Se selecciona el **mejor match por (CODMES, CODSOLICITUD, NOMBRE)**, sin colapsar registros ni persistir nombres.

  
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


%md
## 4. Conversión de montos  
### Tipado a Decimal (valores en bruto)

Los montos provenientes de Salesforce se convierten a tipo **Decimal** para asegurar consistencia numérica.

Alcance de esta etapa:
- No se aplica tipo de cambio.
- No se aplican reglas de autonomía.
- Los montos se mantienen en valores **en bruto**, únicamente tipados.

Esto evita errores en agregaciones y análisis posteriores.

  
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


%md
## 5. Loaders (staging)  
### Lectura y preparación inicial de fuentes

Se realiza la lectura y estandarización inicial de todas las fuentes:

**Orgánico**
- Matrícula del colaborador.
- Matrícula del superior.
- CODMES para alineación temporal.

**Salesforce Estados**
- Múltiples registros por CODSOLICITUD (pasos del flujo).
- Parseo a timestamp real.
- Derivación de CODMESEVALUACION.

**Salesforce Productos**
- Un registro por CODSOLICITUD.
- Atributos de producto y montos.
- Derivación de CODMESCREACION.

**PowerApps**
- Fuente complementaria.
- Motivos y resultados.
- Selección del último registro por solicitud.


  
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


%md
## 6. Enriquecimiento con Orgánico  
### Obtención de matrículas por actor

Se aplica el matching por tokens para obtener matrículas a partir de los distintos campos de actor.

**Desde INFORMES_ESTADOS_*:**
- MAT1: NBRULTACTOR  
- MAT2: NBRULTACTORPASO  

**Desde INFORME_PRODUCTO_*:**
- MAT3: NBRANALISTA  
- MAT4: NBRANALISTAASIGNADO  

Esta etapa expone las matrículas candidatas que serán usadas para la atribución final.

  
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


# =========================================================
# %md
# # 7) Snapshots (1 fila por CODSOLICITUD)
# ## 7.1 Estados: “último evento” define estado vigente
# Ordenamos por FECHORINICIOEVALUACION (timestamp) y luego fin (timestamp) para desempate.
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

# =========================================================
# %md
# ## 7.2 Productos: 1 registro por CODSOLICITUD (tomamos el más reciente por FECCREACION)
# =========================================================
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


%md
## 8. Derivación de PRODUCTO  
### Clasificación TC / CEF

Se clasifica cada solicitud según el tipo de producto (TC o CEF) a partir del campo PROCESO.

Este campo se utiliza para segmentación analítica y construcción de tableros.

  
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


%md
## 9. Atribución del analista  
### MATANALISTA_FINAL y ORIGEN_MATANALISTA

La atribución del analista se resuelve combinando múltiples señales debido a inconsistencias del flujo.

Orden de prioridad:
1. Estados:
   - MAT1 (NBRULTACTOR)
   - MAT2 (NBRULTACTORPASO)
2. Productos:
   - MAT3 (NBRANALISTA)
   - MAT4 (NBRANALISTAASIGNADO)

Se generan:
- MATANALISTA_FINAL  
- ORIGEN_MATANALISTA  

No se evalúan roles (analista / supervisor / gerente) en esta etapa.

  
def build_matanalista_final(df_estados_enriq, df_prod_snap):
    # Base step por tipo (TC/CEF) para atribución desde ESTADOS
    is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
    is_cef = F.col("PROCESO").isin(
        "CO SOLICITUD APROBACIONES TLMK",
        "SFCP APROBACIONES EDUCATIVO",
        "CO SOLICITUD APROBACIONES"
    )

    paso_tc_analista  = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
    paso_cef_analista = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")

    es_paso_base = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

    w_base = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORINICIOEVALUACION").desc_nulls_last())

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
        .drop("rn_base")
    )

    df_matanalista_estados = (
        df_base_latest
        .withColumn(
            "MATANALISTA_ESTADOS",
            F.coalesce(F.col("MAT1_ESTADOS"), F.col("MAT2_ESTADOS"))
        )
        .withColumn(
            "ORIGEN_MATANALISTA_ESTADOS",
            F.when(F.col("MAT1_ESTADOS").isNotNull(), F.lit("ESTADOS_MAT1"))
             .when(F.col("MAT2_ESTADOS").isNotNull(), F.lit("ESTADOS_MAT2"))
             .otherwise(F.lit(None))
        )
        .select("CODSOLICITUD", "MATANALISTA_ESTADOS", "ORIGEN_MATANALISTA_ESTADOS", "TS_BASE_ESTADOS")
    )

    # Productos: ya viene 1 fila por CODSOLICITUD
    df_matanalista_productos = (
        df_prod_snap.select(
            "CODSOLICITUD",
            F.col("MATORGANICO_ANALISTA").alias("MAT3_PRODUCTOS"),
            F.col("MATORGANICO_ASIGNADO").alias("MAT4_PRODUCTOS"),
        )
    )

    df_matanalista_final = (
        df_matanalista_estados
        .join(df_matanalista_productos, on="CODSOLICITUD", how="left")
        .withColumn(
            "MATANALISTA_FINAL",
            F.coalesce(F.col("MATANALISTA_ESTADOS"), F.col("MAT3_PRODUCTOS"), F.col("MAT4_PRODUCTOS"))
        )
        .withColumn(
            "ORIGEN_MATANALISTA",
            F.when(F.col("MATANALISTA_ESTADOS").isNotNull(), F.col("ORIGEN_MATANALISTA_ESTADOS"))
             .when(F.col("MAT3_PRODUCTOS").isNotNull(), F.lit("PRODUCTOS_MAT3"))
             .when(F.col("MAT4_PRODUCTOS").isNotNull(), F.lit("PRODUCTOS_MAT4"))
             .otherwise(F.lit(None))
        )
        .drop("MATANALISTA_ESTADOS", "ORIGEN_MATANALISTA_ESTADOS", "MAT3_PRODUCTOS", "MAT4_PRODUCTOS")
    )

    return df_matanalista_final


%md
## 10. MATSUPERIOR desde Orgánico  
### Jerarquía por mes y matrícula

Se obtiene la matrícula del superior cruzando:
- CODMESEVALUACION
- MATANALISTA_FINAL

Este enfoque permite manejar correctamente cambios de matrícula por mes.


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


%md
## 11. PowerApps como fallback  
### Completitud y motivos

PowerApps se utiliza como fuente complementaria bajo un enfoque **COALESCE**.

Reglas aplicadas:
- Si MATANALISTA_FINAL es null → usar MATANALISTA (PowerApps)
- Si ESTADOSOLICITUDPASO es null → usar RESULTADOANALISTA
- Si NBRPRODUCTO es null → usar PRODUCTO

Se incorporan además:
- MOTIVORESULTADOANALISTA  
- MOTIVOMALADERIVACION  
- SUBMOTIVOMALADERIVACION

  
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


%md
## 12. Pipeline end-to-end  
### Flujo consolidado sin autonomías

Integración final del pipeline:
- Universo anclado a Estados.
- Enriquecimiento con Productos.
- Atribución de analista.
- Fallback con PowerApps.

La salida queda lista para consumo analítico sin lógica de autonomías.


# 12.1 Staging
df_org       = load_organico(spark, BASE_DIR_ORGANICO)
df_org_tokens= build_org_tokens(df_org)

df_estados   = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos = load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps      = load_powerapps(spark, PATH_PA_SOLICITUDES)

# 12.2 Enriquecimiento con orgánico (matrículas)
df_estados_enriq   = enrich_estados_con_organico(df_estados, df_org_tokens)
df_productos_enriq = enrich_productos_con_organico(df_productos, df_org_tokens)

# 12.3 Snapshots (1 fila por solicitud)
df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

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

  
%md
## 13. Escritura a Delta  
### Publicación del resultado final

El resultado se persiste en formato **Delta** con:
- Granularidad 1 fila por CODSOLICITUD.
- Esquema controlado.
- Sin exposición de datos DAC.

La tabla queda disponible para tableros y consultas SQL.


df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.TP_SOLICITUDES_CENTRALIZADO")
