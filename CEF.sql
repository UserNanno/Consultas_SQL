# =========================================================
# X. POWERAPPS — BaseSolicitudes (Apps)
#    - Carga 1n_Apps_*.csv
#    - Normaliza textos
#    - Convierte Created (UTC) a America/Lima
#    - Dedup por CODSOLICITUD (máxima FECASIGNACION)
#    - Mapea MATANALISTA por (CODMES, CORREO) contra Orgánico
# =========================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/BASESOLICITUDES/1n_Apps_*.csv"

df_pa_raw = (
    spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")                 # 👈 en tu salida local era ';'
      .option("encoding", "utf-8")        # o "utf-8-sig" no siempre existe en Spark; con utf-8 suele bastar
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .load(PATH_PA_SOLICITUDES)
)

# 1) Selección + renombre (mismo mapping que pandas)
df_pa = (
    df_pa_raw.select(
        F.col("Title").alias("CODSOLICITUD"),
        F.col("FechaAsignacion").alias("FECASIGNACION"),
        F.col("Tipo de Producto").alias("PRODUCTO"),
        F.col("ResultadoAnalista").alias("RESULTADOANALISTA"),
        F.col("Mail").alias("CORREO"),
        F.col("Motivo Resultado Analista").alias("MOTIVORESULTADOANALISTA"),
        F.col("AñoMes").alias("CODMES"),
        F.col("Created").alias("CREATED"),
        F.col("Motivo_MD").alias("MOTIVOMALADERIVACION"),
        F.col("Submotivo_MD").alias("SUBMOTIVOMALADERIVACION"),
    )
    .withColumn("CODMES", F.col("CODMES").cast("string"))
)

# 2) Parse FECASIGNACION (dayfirst) y Created (UTC->Lima)
#    - FECASIGNACION: en pandas era dayfirst=True; aquí probamos algunos formatos comunes
df_pa = (
    df_pa
      .withColumn("FECASIGNACION_TS",
          F.coalesce(
              F.to_timestamp("FECASIGNACION", "dd/MM/yyyy HH:mm:ss"),
              F.to_timestamp("FECASIGNACION", "dd/MM/yyyy HH:mm"),
              F.to_timestamp("FECASIGNACION", "dd/MM/yyyy"),
              F.to_timestamp("FECASIGNACION")  # fallback
          )
      )
)

# Created: suele venir ISO-8601 tipo 2025-11-28T23:59:00Z o con milis
created_ts = F.to_timestamp("CREATED")  # Spark suele parsear ISO8601; si no, lo ajustamos después
df_pa = (
    df_pa
      .withColumn("CREATED_TS_UTC", created_ts)
      .withColumn("FECHORACREACION", F.from_utc_timestamp(F.col("CREATED_TS_UTC"), "America/Lima"))
      .withColumn("FECCREACION", F.to_date("FECHORACREACION"))
      .withColumn("HORACREACION", F.date_format("FECHORACREACION", "HH:mm:ss"))
)

# 3) Normalización de texto (usa tus helpers Spark: norm_txt_spark/quitar_tildes)
#    OJO: norm_txt_spark espera nombre de columna; aplicamos sobre columnas del DF.
for c in ["PRODUCTO","RESULTADOANALISTA","MOTIVORESULTADOANALISTA","MOTIVOMALADERIVACION","SUBMOTIVOMALADERIVACION"]:
    df_pa = df_pa.withColumn(c, norm_txt_spark(c))

# 4) Homologación RESULTADOANALISTA (igual que pandas replace)
df_pa = (
    df_pa.withColumn(
        "RESULTADOANALISTA",
        F.when(F.col("RESULTADOANALISTA") == "DENEGADO POR ANALISTA DE CREDITO", F.lit("DENEGADO"))
         .when(F.col("RESULTADOANALISTA") == "APROBADO POR ANALISTA DE CREDITO", F.lit("APROBADO"))
         .when(F.col("RESULTADOANALISTA") == "DEVOLVER AL GESTOR", F.lit("DEVUELTO AL GESTOR"))
         .otherwise(F.col("RESULTADOANALISTA"))
    )
)

# 5) Filtrar correos inválidos y mapear MATANALISTA por (CODMES, CORREO) contra Orgánico
df_org_email = (
    df_organico
      .filter((F.col("CORREO").isNotNull()) & (F.col("CORREO") != "-"))
      .select("CODMES", "CORREO", "MATORGANICO", "FECINGRESO")
)

df_pa_email = (
    df_pa
      .filter((F.col("CORREO").isNotNull()) & (F.col("CORREO") != "-"))
      .select("CODSOLICITUD", "CODMES", "CORREO")
      .distinct()
)

df_merge = (
    df_pa_email
      .join(df_org_email, on=["CODMES","CORREO"], how="inner")
)

# Elegir 1 matrícula por (CODMES, CORREO): la de mayor FECINGRESO (igual que tu pandas)
w_email = Window.partitionBy("CODMES","CORREO").orderBy(F.col("FECINGRESO").desc_nulls_last())
df_email_unique = (
    df_merge
      .withColumn("rn", F.row_number().over(w_email))
      .filter(F.col("rn") == 1)
      .select("CODMES","CORREO", F.col("MATORGANICO").alias("MATANALISTA"))
)

# Traer MATANALISTA a PowerApps
df_pa = (
    df_pa
      .join(df_email_unique, on=["CODMES","CORREO"], how="left")
)

# 6) Dedup por CODSOLICITUD: quedarnos con la fila de mayor FECASIGNACION (como pandas sort desc + drop_duplicates)
w_sol = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECASIGNACION_TS").desc_nulls_last())
df_powerapp = (
    df_pa
      .withColumn("rn", F.row_number().over(w_sol))
      .filter(F.col("rn") == 1)
      .drop("rn")
      .select(
          "CODMES","CODSOLICITUD","FECHORACREACION","FECCREACION","HORACREACION",
          "MATANALISTA","FECASIGNACION","PRODUCTO","RESULTADOANALISTA","MOTIVORESULTADOANALISTA",
          "MOTIVOMALADERIVACION","SUBMOTIVOMALADERIVACION"
      )
)

print("PowerApps listo:", df_powerapp.count())
