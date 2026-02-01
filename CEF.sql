DimMes =
VAR MinFecha = DATE(YEAR(MIN('Hechos'[FECSOLICITUD])), MONTH(MIN('Hechos'[FECSOLICITUD])), 1)
VAR MaxFecha = DATE(YEAR(MAX('Hechos'[FECSOLICITUD])), MONTH(MAX('Hechos'[FECSOLICITUD])), 1)
RETURN
ADDCOLUMNS(
    CALENDAR(MinFecha, EOMONTH(MaxFecha, 0)),
    "FechaMes", DATE(YEAR([Date]), MONTH([Date]), 1),
    "CODMES", YEAR([Date]) * 100 + MONTH([Date]),
    "Anio", YEAR([Date]),
    "MesNum", MONTH([Date]),
    "MesTxt", FORMAT([Date], "YYYYMM"),
    "MesNombre", FORMAT([Date], "MMM YYYY"),
    "MesIndex", YEAR([Date]) * 12 + MONTH([Date])
)
