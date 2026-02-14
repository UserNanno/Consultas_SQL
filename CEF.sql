Slicer Dias Utiles =
GENERATESERIES(0.5, 31, 0.5)


Filtro Dias Utiles =
VAR MaxSeleccionado =
    MAX('Slicer Dias Utiles'[Value])
RETURN
IF(
    MAX(Fact[ACUM_DIA_UTIL_EQ_MES]) <= MaxSeleccionado
        && MAX(Fact[PESO_DIA_UTIL_EQ]) > 0,
    1,
    0
)
