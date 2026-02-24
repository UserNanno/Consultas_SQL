Este es el contexto de ORGANICO.csv de como lo creee antes

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

BASE_DIR_ORGANICO   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/1n_Activos_*.csv"
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
    Normalización:
    - upper
    - quitar tildes de vocales (no toca Ñ)
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

    # Mantener A-Z, Ñ, dígitos y espacio
    c = F.regexp_replace(c, r"[^A-ZÑ0-9 ]", " ")

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





from pyspark.sql import functions as F

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
        F.col("Nombre Corto").alias("NBRCORTO"),
    )
    df = norm_col(df, ["MATORGANICO", "NOMBRECOMPLETO", "MATSUPERIOR", "NBRCORTO"])

    df = df.withColumn("MATSUPERIOR", F.regexp_replace("MATSUPERIOR", r"^0(?=[A-Z]\d{5})", ""))

    df = df.withColumn(
        "FECINGRESO_CLEAN",
        F.regexp_replace(F.trim(F.col("FECINGRESO")), r"[-\.]", "/")
    )

    formatos = [
        "d/M/yyyy",
        "dd/MM/yyyy",
        "d/M/yy",
        "M/d/yyyy",
        "yyyy-MM-dd"
    ]

    parsed_cols = [F.to_date(F.col("FECINGRESO_CLEAN"), fmt) for fmt in formatos]
    df = df.withColumn("FECINGRESO_DT", F.coalesce(*parsed_cols))

    df = (
        df.drop("FECINGRESO")
          .drop("FECINGRESO_CLEAN")
          .withColumnRenamed("FECINGRESO_DT", "FECINGRESO")
    )

    # Array de NOMBRECOMPLETO
    df = df.withColumn(
        "NOMBRE_TOKENS_RAW",
        F.split(F.col("NOMBRECOMPLETO"), r"\s+")
    ).withColumn(
        "NOMBRE_TOKENS",
        F.expr("filter(NOMBRE_TOKENS_RAW, x -> x <> '')")
    ).drop("NOMBRE_TOKENS_RAW")

    return df






from pyspark.sql import functions as F

# Stopwords en MAYÚSCULAS (consistentes con norm_txt)
STOPWORDS_ES = ["DE", "DEL", "LA", "LOS", "LAS", "Y", "E", "O", "U", "EL", "DA", "DO", "DOS", "A"]

def build_diccionario_actores(
    spark,
    path_estados,
    path_productos,
    filtrar_stopwords=True
):
    # ESTADOS (Salesforce)
    df_est_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_estados)
    )

    df_est = df_est_raw.select(
        F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION_RAW"),
        F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
        F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
    )

    # Normalizar + limpiar
    df_est = norm_col(df_est, ["NBRULTACTOR", "NBRULTACTORPASO"])
    df_est = (
        df_est.withColumn("NBRULTACTOR", limpiar_cesado(F.col("NBRULTACTOR")))
              .withColumn("NBRULTACTORPASO", limpiar_cesado(F.col("NBRULTACTORPASO")))
    )

    # Fecha → CODMES (yyyyMM)
    df_est = (
        df_est.withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp(F.col("FECINICIOEVALUACION_RAW")))
              .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
              .withColumn("CODMESEVALUACION", F.date_format("FECINICIOEVALUACION", "yyyyMM").cast("string"))
              .drop("FECINICIOEVALUACION_RAW", "FECHORINICIOEVALUACION", "FECINICIOEVALUACION")
    )

    # Apilar nombres de estados -> (NOMBRE, CODMES)
    dicc_estados = (
        df_est.select(F.col("NBRULTACTOR").alias("NOMBRE"), F.col("CODMESEVALUACION").alias("CODMES"))
              .unionByName(df_est.select(F.col("NBRULTACTORPASO").alias("NOMBRE"), F.col("CODMESEVALUACION").alias("CODMES")))
              .where(F.col("NOMBRE").isNotNull() & (F.col("NOMBRE") != ""))
    )

    # Dedup por (NOMBRE, CODMES)
    dicc_estados = dicc_estados.dropDuplicates(["NOMBRE", "CODMES"])

    # PRODUCTOS
    df_prod_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_productos)
    )

    df_prod = df_prod_raw.select(
        F.col("Analista").alias("NBRANALISTA"),
        F.col("Analista de crédito").alias("NBRANALISTAASIGNADO"),
        F.col("Fecha de creación").alias("FECCREACION_RAW")
    )

    # Normalizar + limpiar
    df_prod = norm_col(df_prod, ["NBRANALISTA","NBRANALISTAASIGNADO"])
    df_prod = (
        df_prod.withColumn("NBRANALISTA", limpiar_cesado(F.col("NBRANALISTA")))
               .withColumn("NBRANALISTAASIGNADO", limpiar_cesado(F.col("NBRANALISTAASIGNADO")))
    )

    # Fecha -> CODMES (yyyyMM)
    df_prod = (
        df_prod.withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION_RAW").cast("string")))
               .withColumn("FECCREACION",
                   F.coalesce(
                       F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                       F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                       F.to_date("FECCREACION_STR")
                   )
               )
               .withColumn("CODMESCREACION", F.date_format("FECCREACION", "yyyyMM").cast("string"))
               .drop("FECCREACION_RAW", "FECCREACION_STR", "FECCREACION")
    )

    dicc_productos = (
        df_prod.select(F.col("NBRANALISTA").alias("NOMBRE"), F.col("CODMESCREACION").alias("CODMES"))
               .unionByName(df_prod.select(F.col("NBRANALISTAASIGNADO").alias("NOMBRE"), F.col("CODMESCREACION").alias("CODMES")))
               .where(F.col("NOMBRE").isNotNull() & (F.col("NOMBRE") != ""))
    )

    dicc_productos = dicc_productos.dropDuplicates(["NOMBRE", "CODMES"])

    # UNIFICAR DICCIONARIO Y DEDUP FINAL
    dicc_maestro = (
        dicc_estados.unionByName(dicc_productos)
                    .where(F.col("CODMES").isNotNull())  # opcional; quita filas sin mes
                    .dropDuplicates(["NOMBRE", "CODMES"])
    )

    # TOKENS
    dicc_maestro = (
        dicc_maestro
        .withColumn("NOMBRE_TOKENS", F.split(F.col("NOMBRE"), r"\s+"))
        .withColumn("NOMBRE_TOKENS", F.array_remove(F.col("NOMBRE_TOKENS"), ""))
    )

    if filtrar_stopwords and STOPWORDS_ES:
        sw_array = F.array(*[F.lit(w) for w in STOPWORDS_ES])
        dicc_maestro = dicc_maestro.withColumn(
            "NOMBRE_TOKENS_NSW",
            F.array_except(F.col("NOMBRE_TOKENS"), sw_array)
        )
    else:
        dicc_maestro = dicc_maestro.withColumn("NOMBRE_TOKENS_NSW", F.col("NOMBRE_TOKENS"))

    return dicc_maestro




df_org = load_organico(spark, BASE_DIR_ORGANICO)
df_diccionario = build_diccionario_actores(
    spark,
    path_estados=PATH_SF_ESTADOS,
    path_productos=PATH_SF_PRODUCTOS,
    filtrar_stopwords=True
)





from pyspark.sql import functions as F
from pyspark.sql import Window

def map_diccionario_a_organico(
    df_diccionario,
    df_org,
    min_tokens=3,
    usar_stopwords=True,
    alinear_mes=True,
    devolver_todos=False
):
    """
    Une df_diccionario con df_org por coincidencia de tokens (arrays) y devuelve
    MATORGANICO, MATSUPERIOR y CORREO para cada (NOMBRE, CODMES) del diccionario,
    siempre que compartan al menos `min_tokens` tokens.

    Parámetros:
      - min_tokens: umbral mínimo de tokens en común (por defecto 3).
      - usar_stopwords: si True intenta usar columnas *_NSW si existen.
      - alinear_mes: si True, une sólo dentro del mismo CODMES.
      - devolver_todos: si False devuelve el mejor match por (NOMBRE, CODMES).
                        Si True devuelve todos los matches por encima del umbral.
    """

    # Elegir columnas de tokens (preferir *_NSW si existen y usar_stopwords=True)
    dicc_tok_col = "NOMBRE_TOKENS_NSW" if (usar_stopwords and "NOMBRE_TOKENS_NSW" in df_diccionario.columns) else "NOMBRE_TOKENS"
    org_tok_col  = "NOMBRE_TOKENS_NSW" if (usar_stopwords and "NOMBRE_TOKENS_NSW" in df_org.columns) else "NOMBRE_TOKENS"

    # Sanidad mínima
    for df_name, df_obj, col_name in [
        ("df_diccionario", df_diccionario, dicc_tok_col),
        ("df_org", df_org, org_tok_col),
    ]:
        if col_name not in df_obj.columns:
            raise ValueError(f"{df_name} no tiene la columna de tokens esperada: {col_name}")

    # 2) Explode tokens para crear índice invertido y evitar duplicados de token por entidad
    dicc_tok = (
        df_diccionario
        .select(
            F.col("NOMBRE"),
            F.col("CODMES"),
            F.col(dicc_tok_col).alias("TOKENS_D")
        )
        .withColumn("TOKEN", F.explode("TOKENS_D"))
        .where(F.col("TOKEN") != "")
        .dropDuplicates(["NOMBRE", "CODMES", "TOKEN"])
    )

    org_tok = (
        df_org
        .select(
            F.col("MATORGANICO"),
            F.col("MATSUPERIOR"),
            F.col("CORREO"),
            F.col("NBRCORTO"),
            F.col("CODMES"),
            F.col(org_tok_col).alias("TOKENS_O")
        )
        .withColumn("TOKEN", F.explode("TOKENS_O"))
        .where(F.col("TOKEN") != "")
        .dropDuplicates(["MATORGANICO", "CODMES", "TOKEN"])
    )

    # Join por TOKEN (+ CODMES si alinear_mes=True)
    join_keys = ["TOKEN", "CODMES"] if alinear_mes else ["TOKEN"]
    join_tok = dicc_tok.alias("d").join(org_tok.alias("o"), on=join_keys, how="inner")

    # Conteo de tokens en común por par (NOMBRE, CODMES, MATORGANICO,...)
    matches = (
        join_tok
        .groupBy(
            F.col("d.NOMBRE").alias("NOMBRE"),
            F.col("d.CODMES").alias("CODMES"),
            F.col("o.MATORGANICO").alias("MATORGANICO"),
            F.col("o.MATSUPERIOR").alias("MATSUPERIOR"),
            F.col("o.CORREO").alias("CORREO"),
            F.col("o.NBRCORTO").alias("NBRCORTO")
        )
        .agg(F.countDistinct("TOKEN").alias("TOKENS_MATCH"))
    )

    # Metricas extra: tamaños y Jaccard aproximado
    dicc_sizes = df_diccionario.select(
        "NOMBRE", "CODMES",
        F.size(F.col(dicc_tok_col)).alias("D_SIZE")
    ).dropDuplicates(["NOMBRE", "CODMES"])

    org_sizes = df_org.select(
        "MATORGANICO", "CODMES",
        F.size(F.col(org_tok_col)).alias("O_SIZE")
    ).dropDuplicates(["MATORGANICO", "CODMES"])

    matches = (matches
        .join(dicc_sizes, on=["NOMBRE", "CODMES"], how="left")
        .join(org_sizes, on=["MATORGANICO", "CODMES"], how="left")
        .withColumn("UNION_SIZE", (F.col("D_SIZE") + F.col("O_SIZE") - F.col("TOKENS_MATCH")))
        .withColumn("JACCARD", F.when(F.col("UNION_SIZE") > 0, F.col("TOKENS_MATCH") / F.col("UNION_SIZE")).otherwise(F.lit(0.0)))
    )

    # Filtro por umbral de tokens
    matches_filtered = matches.filter(F.col("TOKENS_MATCH") >= F.lit(min_tokens))

    # Devolver mejor match por (NOMBRE, CODMES) o todos
    if devolver_todos:
        return matches_filtered.orderBy(F.desc("TOKENS_MATCH"), F.desc("JACCARD"))
    else:
        w = Window.partitionBy("NOMBRE", "CODMES").orderBy(F.desc("TOKENS_MATCH"), F.desc("JACCARD"))
        best = matches_filtered.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
        return best






matches_top = map_diccionario_a_organico(
    df_diccionario=df_diccionario,
    df_org=df_org,
    min_tokens=3,         # al menos 3 tokens en común
    usar_stopwords=True,  # usa *_NSW si existen
    alinear_mes=True,     # compara dentro del mismo CODMES
    devolver_todos=False  # devuelve solo el mejor por (NOMBRE, CODMES)
)





final_dir = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/ORGANICO/"
tmp_dir   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/ORGANICO/.tmp_export/"

(matches_top
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ";")
    .option("quote", '"')
    .option("escape", '"')
    .option("nullValue", "")
    .csv(tmp_dir)
)







# Encuentra el part-*.csv
part_files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-") and f.name.endswith(".csv")]
assert len(part_files) == 1, f"Esperaba 1 part-*.csv, encontré {len(part_files)}"
part_file = part_files[0]

dbutils.fs.mkdirs(final_dir)
for f in dbutils.fs.ls(final_dir):
    if f.name.endswith(".csv"):
        dbutils.fs.rm(f.path, True)

custom_name = "ORGANICO.csv"
dest_file = f"{final_dir}{custom_name}"

dbutils.fs.mv(part_file, dest_file)
dbutils.fs.rm(tmp_dir, True)
