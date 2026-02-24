def parse_created_utc(col):
    """
    Parsea PowerApps Created en formato:
    - 2026-02-02T15:02:04Z
    - 2026-02-02T15:02:04.000Z (por si aparece)
    Devuelve timestamp en UTC.
    """
    s = col.cast("string")

    return F.coalesce(
        F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )




df = df.withColumn("CREATED_TS_UTC", parse_created_utc(F.col("CREATED")))

df = df.withColumn(
    "CREATED_LIMA",
    F.from_utc_timestamp(F.col("CREATED_TS_UTC"), "America/Lima")
)
