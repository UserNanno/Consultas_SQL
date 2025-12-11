from pyspark.sql import functions as F

# ... tu código de lectura y normalización de textos se mantiene igual ...

df_salesforce_productos = (
    df_salesforce_productos
        # limpiar espacios raros y asegurar string
        .withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION").cast("string")))
        # intentar varios formatos posibles
        .withColumn(
            "FECCREACION_DATE",
            F.coalesce(
                F.to_date("FECCREACION_STR", "dd/MM/yyyy"),   # ej: 31/01/2025
                F.to_date("FECCREACION_STR", "yyyy-MM-dd"),   # ej: 2025-01-31
                F.to_date("FECCREACION_STR")                  # fallback por defecto
            )
        )
        .withColumn("CODMESCREACION", F.date_format("FECCREACION_DATE", "yyyyMM"))
)


df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("FECCREACION", F.col("FECCREACION_DATE"))
        .drop("FECCREACION_STR", "FECCREACION_DATE")
)
