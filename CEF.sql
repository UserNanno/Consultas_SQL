# 1️⃣ Cantidad de registros en el DF original
count_original = df_salesforce_estados.count()
print("📌 Registros en df_salesforce_estados (original):", count_original)

# 2️⃣ Cantidad de registros únicos (solo el último por solicitud)
count_unicos = df_salesforce_estados_unicos.count()
print("📌 Registros en df_salesforce_estados_unicos:", count_unicos)

# 3️⃣ Cantidad de registros descartados (los repetidos)
count_descartados = df_salesforce_estados_descartados.count()
print("📌 Registros en df_salesforce_estados_descartados:", count_descartados)


# 4️⃣ Cantidad de repetidos por CODSOLICITUD
df_repetidos_count = (
    df_salesforce_estados
        .groupBy("CODSOLICITUD")
        .count()
        .filter(F.col("count") > 1)
)

count_codsolicitud_repetidos = df_repetidos_count.count()

print("📌 Cantidad de CODSOLICITUD con repeticiones:", count_codsolicitud_repetidos)
print("📌 Ejemplo de valores repetidos:")
df_repetidos_count.show(10, truncate=False)
