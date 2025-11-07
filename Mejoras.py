Mira tengo este dataframe tp_powerapp_clean.head(2) donde tengo estas columnas (CODSOLICITUD FECASIGNACION PRODUCTO RESULTADOANALISTA CORREO MOTIVORESULTADOANALISTA CODMES CREATED MOTIVOMALADERIVACION SUBMOTIVOMALADERIVACION FECCREACION HORACREACION FECHORACREACION) Que son los registros que llenan ciertos analistas También tengo este otro dataframe df_organico.head(2) que tiene estas columnas (CODMES MATORGANICO AREATRIBUCOE SERVICIOTRIBUCOE UNIDADORGANIZATIVA CORREO AGENCIA FUNCION MATSUPERIOR) Que es el registro de los datos de los colaboradores del banco, no solo analistas, si no el total. Lo que necesito es crear un dataframe o quizá en el mismo tp_powerapp_clean incorporar la matrícula (MATORGANICO) de df_organico. Ahora en este df_organico tenemos la info de un colaborador en cierto CODMES. Es normal que en el mes 202507 tenga un MATORGANICO distinto al 202510 porque cambio de puesto, incluso pasa con el correo. Por ello usemos como llave de cruce el CODMES y el correo que son valores que tenemos en ambas tablas. Se podrá? Porque no quiero tener repetidos en mi tp_powerapp_clean


Hice esto, esta bien?
tp_powerapp_clean["CODMES"] = tp_powerapp_clean["CODMES"].astype(str)
df_organico["CODMES"] = df_organico["CODMES"].astype(str)

tp_powerapp_clean["CORREO"] = tp_powerapp_clean["CORREO"].str.lower().str.strip()
df_organico["CORREO"] = df_organico["CORREO"].str.lower().str.strip()
tp_powerapp_clean = tp_powerapp_clean.merge(
    df_organico[["CODMES", "CORREO", "MATORGANICO"]],
    on=["CODMES", "CORREO"],
    how="left"
)

CODSOLICITUD                       object
FECASIGNACION              datetime64[ns]
PRODUCTO                           object
RESULTADOANALISTA                  object
CORREO                             object
MOTIVORESULTADOANALISTA            object
CODMES                             object
CREATED                            object
MOTIVOMALADERIVACION               object
SUBMOTIVOMALADERIVACION            object
FECCREACION                        object
HORACREACION                       object
FECHORACREACION            datetime64[ns]
MATORGANICO                        object

fecasignacion
2025-12-09

feccreacion
2025-09-12
