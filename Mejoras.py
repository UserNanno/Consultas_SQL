def quitar_tildes(col):
    return (F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(col,
        "[ÁÀÂÄáàâä]", "A"),"[ÉÈÊËéèêë]", "E"),"[ÍÌÎÏíìîï]", "I"),"[ÓÒÔÖóòôö]", "O"),"[ÚÙÛÜúùûü]", "U"))




df_salesforce = (
    df_salesforce
        .withColumn("NBRPASO", quitar_tildes("NBRPASO"))
        .withColumn("NBRANALISTA", quitar_tildes("NBRANALISTA"))
        .withColumn("PROCESO", quitar_tildes("PROCESO"))
)
