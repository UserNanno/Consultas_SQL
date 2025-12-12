df_salesforce_invalid = df_salesforce_producto[
    ~(
        df_salesforce_producto["CODSOLICITUD"].str.len().eq(11) &
        df_salesforce_producto["CODSOLICITUD"].str.startswith("O00")
    )
]


df_salesforce_producto = df_salesforce_producto[
    df_salesforce_producto["CODSOLICITUD"].str.len().eq(11) &
    df_salesforce_producto["CODSOLICITUD"].str.startswith("O00")
]


df_salesforce_producto["CODSOLICITUD"].duplicated().sum()
