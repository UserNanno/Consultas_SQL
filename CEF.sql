from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)
from pyspark.sql.functions import col, upper, lpad, to_date
from datetime import datetime

df_base = spark.table("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_DASH_SOLICITUDES_SCRM")
csv_organico = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/ORGANICO/ORGANICO.csv"

df_orga = (spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load(csv_organico))


# LEFT JOIN
df_joined = (
    df_base.alias("a")
    .join(
        df_orga.alias("b"),
        (col("a.CODMES") == col("b.CODMES")) &
        (col("a.MATANALISTA") == col("b.MATORGANICO")),
        "left"
    )
    .select(
        col("a.*"),
        col("b.NBRCORTO")
    )
)




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


# Encuentra el part-*.csv
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


%sql
DROP TABLE IF EXISTS CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_DASH_SOLICITUDES_SCRM;
