from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_base = spark.table("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_DASH_SOLICITUDES_SCRM")

csv_organico = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/ORGANICO/ORGANICO.csv"

df_orga_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "ISO-8859-1")  # si tu export es utf-8, cambia a utf-8
    .load(csv_organico)
)

# 1) Normaliza llaves (muy importante: mismos tipos)
df_orga_key = (
    df_orga_raw
    .select(
        F.col("CODMES").cast("string").alias("CODMES"),
        F.col("MATORGANICO").cast("string").alias("MATORGANICO"),
        F.col("NBRCORTO").alias("NBRCORTO"),
        F.col("TOKENS_MATCH").cast("int").alias("TOKENS_MATCH"),
        F.col("JACCARD").cast("double").alias("JACCARD"),
        F.col("CORREO").alias("CORREO"),
        F.col("NOMBRE").alias("NOMBRE"),
    )
    .withColumn("CODMES", F.regexp_replace("CODMES", r"[^0-9]", ""))
    .withColumn("CODMES", F.when(F.length("CODMES") >= 6, F.substring("CODMES", 1, 6)).otherwise(F.col("CODMES")))
    .withColumn("MATORGANICO", F.trim(F.col("MATORGANICO")))
)

# 2) Dedup a 1 fila por (CODMES, MATORGANICO)
#    Criterio recomendado:
#    - mayor TOKENS_MATCH
#    - mayor JACCARD
#    - NBRCORTO no nulo
#    - CORREO no nulo (si ayuda)
w = Window.partitionBy("CODMES", "MATORGANICO").orderBy(
    F.col("TOKENS_MATCH").desc_nulls_last(),
    F.col("JACCARD").desc_nulls_last(),
    F.col("NBRCORTO").isNotNull().desc(),
    F.col("CORREO").isNotNull().desc(),
    F.col("NOMBRE").asc_nulls_last()
)

df_orga = (
    df_orga_key
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn", "TOKENS_MATCH", "JACCARD", "CORREO", "NOMBRE")
)

# (opcional) Asegura unicidad (debug rápido)
# df_orga.groupBy("CODMES","MATORGANICO").count().filter("count>1").show(50, False)

# 3) Normaliza llaves del base para que matchee
df_base_key = (
    df_base
    .withColumn("CODMES", F.col("CODMES").cast("string"))
    .withColumn("CODMES", F.regexp_replace("CODMES", r"[^0-9]", ""))
    .withColumn("CODMES", F.when(F.length("CODMES") >= 6, F.substring("CODMES", 1, 6)).otherwise(F.col("CODMES")))
    .withColumn("MATANALISTA", F.trim(F.col("MATANALISTA").cast("string")))
)

# 4) LEFT JOIN sin multiplicación
df_joined = (
    df_base_key.alias("a")
    .join(
        df_orga.alias("b"),
        (F.col("a.CODMES") == F.col("b.CODMES")) &
        (F.col("a.MATANALISTA") == F.col("b.MATORGANICO")),
        "left"
    )
    .select(
        F.col("a.*"),
        F.col("b.NBRCORTO")
    )
)

# ---- (debug recomendado) ----
# print("base:", df_base.count())
# print("joined:", df_joined.count())

# -----------------------------
# EXPORT CSV
# -----------------------------
final_dir = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/WEEKLY/"
tmp_dir   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/WEEKLY/.tmp_export/"

(df_joined
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

part_files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-") and f.name.endswith(".csv")]
assert len(part_files) == 1, f"Esperaba 1 part-*.csv, encontré {len(part_files)}"
part_file = part_files[0]

dbutils.fs.mkdirs(final_dir)
for f in dbutils.fs.ls(final_dir):
    if f.name.endswith(".csv"):
        dbutils.fs.rm(f.path, True)

custom_name = "RPT_INDICADORES_CENTRALIZADO.csv"
dest_file = f"{final_dir}{custom_name}"

dbutils.fs.mv(part_file, dest_file)
dbutils.fs.rm(tmp_dir, True)

# Si esta tabla es staging, ok borrarla después:
spark.sql("DROP TABLE IF EXISTS CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_DASH_SOLICITUDES_SCRM")
