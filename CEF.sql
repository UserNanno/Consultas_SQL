from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from pyspark import StorageLevel

# -----------------------------
# PATHS (USANDO EXPORTS)
# -----------------------------
PATH_PLANCHON = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/MIGRACION_PLANCHON/PLANCHON_EDV.csv"
PATH_PA_EXPORT = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/POWERAPPS/POWERAPP.csv"

# -----------------------------
# HELPERS
# -----------------------------
def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def norm_txt(col):
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

def to_decimal_monto(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.trim(s)
    s = F.regexp_replace(s, r"\s+", "")
    s = F.when((F.instr(s, ",") > 0) & (F.instr(s, ".") > 0), F.regexp_replace(s, r"\.", "")).otherwise(s)
    s = F.when(F.instr(s, ",") > 0, F.regexp_replace(s, ",", ".")).otherwise(s)
    return s.cast(DecimalType(18, 2))

# -----------------------------
# LOADERS
# -----------------------------
def load_planchon(spark, path_planchon):
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_planchon)
    )

    df = df_raw.select(
        F.col("NroSolicitud").cast("string").alias("CODSOLICITUD"),
        F.col("TipoOperacion").alias("TIPOPERACION"),
        F.col("Producto").alias("PRODUCTO"),
        F.col("Campana").alias("DESCAMPANIA"),
        F.col("TipoEvaluacion").alias("CENTROATENCION"),
        F.col("MatEvaluador").alias("MATANALISTA"),
        F.col("TipoRenta").alias("TIPRENTA"),
        F.col("SegmentoTitular").alias("SEGMENTO"),
        F.col("Equipo").alias("NBREQUIPO"),
        F.col("Monto desembolsado").alias("MTODESEMBOLSADO"),
        F.col("DESCRIPCION_CAMPANIA").alias("DESTIPEVALUACIONRIESGO"),
        F.col("fecdesembolso").alias("FECDESEMBOLSO"),
        F.col("Canal").alias("CANAL"),
        F.col("EstadoFinal").alias("ESTADOSOLICITUD"),
        F.col("Flag_Desembolso").alias("FLGDESEMBOLSO"),
        F.col("CODMES").cast("int").alias("CODMES"),
        F.col("MontoSolicitadoNUEVO").alias("MTOSOLICITADO"),
        F.col("MontoAprobadoSOLESNUEVO").alias("MTOAPROBADO"),
    )

    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD")))
    df = norm_col(df, ["TIPOPERACION", "CENTROATENCION", "SEGMENTO", "NBREQUIPO", "CANAL"])

    df = df.withColumn("FECDESEMBOLSO", F.to_date("FECDESEMBOLSO", "d/M/yyyy"))
    return df

def load_powerapps_exports(spark, path_apps_export):
    """
    EXPORTS/POWERAPP.csv:
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
            F.col("CODSOLICITUD").cast("string").alias("CODSOLICITUD"),
            F.col("MOTIVORESULTADOANALISTA").alias("MOTIVORESULTADOANALISTA"),
            F.col("MOTIVOMALADERIVACION").alias("MOTIVOMALADERIVACION"),
            F.col("SUBMOTIVOMALADERIVACION").alias("SUBMOTIVOMALADERIVACION"),
        )
    )

    # En EXPORT ya debe venir dedup, pero igual dejamos trim
    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD")))

    # Por seguridad extrema: dedup por CODSOLICITUD (si hubiera duplicados)
    return df.dropDuplicates(["CODSOLICITUD"])

def apply_powerapps(df_planchon, df_apps_export):
    return df_planchon.join(df_apps_export, on="CODSOLICITUD", how="left")

# -----------------------------
# EJECUCIÓN
# -----------------------------
df_planchon = load_planchon(spark, PATH_PLANCHON)

df_planchon = (
    df_planchon
    .withColumn("MTOSOLICITADO",   to_decimal_monto(F.col("MTOSOLICITADO")))
    .withColumn("MTOAPROBADO",     to_decimal_monto(F.col("MTOAPROBADO")))
    .withColumn("MTODESEMBOLSADO", to_decimal_monto(F.col("MTODESEMBOLSADO")))
)

df_planchon = df_planchon.withColumn(
    "TIPRENTA2",
    F.when(F.col("TIPRENTA") == "0", "0")
     .when(F.col("TIPRENTA") == "1", "1RA")
     .when(F.col("TIPRENTA") == "2", "2DA")
     .when(F.col("TIPRENTA") == "3", "3RA")
     .when(F.col("TIPRENTA") == "4", "4TA")
     .when(F.col("TIPRENTA") == "5", "5TA")
     .when(F.col("TIPRENTA") == "6", "6TA")
     .when(F.col("TIPRENTA").isNull() | F.col("TIPRENTA").isin("null", "NULL", "Sin data"), None)
)

df_planchon = df_planchon.withColumn(
    "ESTADOSOLICITUD2",
    F.when(F.col("ESTADOSOLICITUD") == "ACEPTADAS", "APROBADO")
     .when(F.col("ESTADOSOLICITUD") == "DENEGADAS", "RECHAZADO")
     .when(F.col("FLGDESEMBOLSO").isNull(), "RECHAZADO")
)

df_planchon = df_planchon.withColumn(
    "CENTROATENCION2",
    F.when(F.col("CENTROATENCION") == "EVALUACION EN CENTRALIZADO", "CENTRALIZADO")
     .when(F.col("CENTROATENCION") == "EVALUACION AUTOMATICA", "PUNTO DE CONTACTO")
)

df_planchon = df_planchon.withColumn(
    "DESTIPOPERACION",
    F.when(F.col("TIPOPERACION") == "CREDITO CONSUMO NUEVO", "CREDITO CONSUMO")
     .when(F.col("TIPOPERACION") == "TARJETA CREDITO NUEVA", "TARJETA CREDITO NUEVA")
     .when(F.col("TIPOPERACION") == "TARJETA CREDITO STOCK", "TARJETA CREDITO STOCK")
)

df_planchon = df_planchon.withColumn(
    "NBREQUIPO2",
    F.when(F.col("NBREQUIPO") == "EQUIPO CONSUMO 2 EVELYN CHAVEZ", "EQUIPO EC")
     .when(F.col("NBREQUIPO") == "EQUIPO CONSUMO 3 MARLENE DEL AGUILA", "EQUIPO MA")
     .when(F.col("NBREQUIPO") == "EQUIPO CONSUMO 1 PAOLA MONTALVA", "EQUIPO PM")
     .when(F.col("NBREQUIPO") == "G CREDITICIO GIOVANA PINEDA", "EQUIPO GP")
     .when(F.col("NBREQUIPO") == "EQUIPO PYME CLIVER MOREY", "EQUIPO CM")
     .when(F.col("NBREQUIPO") == "EQUIPO 3RA", "EQUIPO EQUIPO 3RA")
     .when(F.col("NBREQUIPO") == "EQUIPO STOCK", "EQUIPO STOCK")
     .when(F.col("NBREQUIPO") == "EQUIPOS ESPEJO", "EQUIPOS ESPEJOS")
)

df_planchon = df_planchon.withColumn(
    "FLGDESEMBOLSO2",
    F.when(F.col("FLGDESEMBOLSO") == "Desembolsado", 1)
     .when(F.col("FLGDESEMBOLSO") == "No Desembolsado", 0)
     .when(F.col("FLGDESEMBOLSO").isNull(), 0)
)

df_planchon = (
    df_planchon
    .drop("ESTADOSOLICITUD", "PRODUCTO", "CENTROATENCION", "TIPOPERACION", "NBREQUIPO", "FLGDESEMBOLSO", "TIPRENTA")
    .withColumnRenamed("ESTADOSOLICITUD2", "ESTADOSOLICITUD")
    .withColumnRenamed("CENTROATENCION2", "CENTROATENCION")
    .withColumnRenamed("NBREQUIPO2", "NBREQUIPO")
    .withColumnRenamed("FLGDESEMBOLSO2", "FLGDESEMBOLSO")
    .withColumnRenamed("TIPRENTA2", "DESTIPRENTA")
)

df_apps_export = load_powerapps_exports(spark, PATH_PA_EXPORT)
df_final = apply_powerapps(df_planchon, df_apps_export)

# (Opcional) materializa antes de escribir si el write te tarda por lineage
df_out = df_final.persist(StorageLevel.DISK_ONLY)
_ = df_out.count()

(df_out.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_TP_PLANCHON_1")
)

df_out.unpersist()
