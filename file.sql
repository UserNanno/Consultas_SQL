ACUMULADOSOLICITUDES_ULTIMODIA =
VAR DiaActual = MAX('WEEKLY'[NUMDIA])
VAR UltimoDiaMes =
    CALCULATE(
        MAX('WEEKLY'[NUMDIA]),
        ALLSELECTED('WEEKLY'[NUMDIA])
    )
RETURN
IF(
    DiaActual = UltimoDiaMes,
    [ACUMULADOSOLICITUDES],   -- usamos tu medida acumulada
    BLANK()
)
