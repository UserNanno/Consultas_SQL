Tengo este dataframe
tp_powerapp_clean.head(2)
dodne tengo estas columnas (CODSOLICITUD	FECASIGNACION	PRODUCTO	RESULTADOANALISTA	CORREO	MOTIVORESULTADOANALISTA	CODMES	CREATED	MOTIVOMALADERIVACION	SUBMOTIVOMALADERIVACION	FECCREACION	HORACREACION	FECHORACREACION)

También tengo este otro dataframe
df_organico.head(2)
donde tengo estas columnas (CODMES	FECINGRESO MATORGANICO	NOMBRECOMPLETO	AREATRIBUCOE	SERVICIOTRIBUCOE	UNIDADORGANIZATIVA	CORREO	AGENCIA	FUNCION	MATSUPERIOR)
Acá en el campo Correo hay valores "-" deberiamos quitarlos.

Paso 1: Lo que necesito primero es buscar todos los crreos que aparecen en tp_powerapp_clean y en qué mes
Paso 2: Luego debo verificar que todos esos correos los tenga en df_organico en el mismo mes que se encontré en tp_powerapp_clean
Paso 3: Si existe coincidencia vamos a crear un dataframe nuevo donde tengamos un solo registro por CODMES, MATORGANICO y CORREO (tomaremos el correo mas actual por FECINGRESO en ese CODMES). Acá verificar que por ejemplo si bien hay dos matrículas distintas, puede tener un mismo correo y viceverwsa. Quedarse con el de FECINGRESO mas actual para ese CODMES
Paso 3: Luego que se haya garantizado que ese nuevo dataframe tenga valores unicos por matrícual y correo en el CODMES se debe agregar a tp_powerapp_clean el MATORGANICO y renombrarlo como MATANALISTA
