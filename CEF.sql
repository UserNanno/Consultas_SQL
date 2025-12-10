df_salesforce_productos = (
    df_salesforce_raw
        .select(
            F.col("Nombre de la oportunidad").alias("CODOPORTUNIDAD"),
            F.col("Nombre del Producto").alias("NBRPRODUCTO"),
            F.col("Etapa").alias("ETAPA"),
            F.col("Analista de crédito").alias("NBRANALISTA"),
            F.col("Tipo de Acción").alias("TIPACCION")
        )
)


Tengo este otro df donde quiero hacer lo mismo con NBRANALISTA que haciamos en df_salesforce_estados para buscar su MATORGANICO y colocarlo acá. No habría problema que haya por ejemplo haya 3 nombres en algunos registros donde se repite el segundo nombre con nuestra forma de identificar con la tolerancia, no habria problemas no para aplicar lo que ya tenemos?
