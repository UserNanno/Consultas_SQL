FLGVENTANA = 
VAR MesBaseIndex =
   MAXX(ALLSELECTED(DimMes), DimMes[MesIndex])
VAR MesEjeIndex =
   SELECTEDVALUE(DimMes[MesIndex])
RETURN
IF(
   MesEjeIndex >= MesBaseIndex - 3
&& MesEjeIndex <= MesBaseIndex,
   CALCULATE(
       [CTDSOLICITUDES],
       REMOVEFILTERS(DimMes[CODMES])
   ),
   BLANK()
)



DIMMES = 
VAR Base =
   DISTINCT( SELECTCOLUMNS('WEEKLY', "CODMES", 'WEEKLY'[CODMES]) )
RETURN
ADDCOLUMNS(
   Base,
   "Anio", INT([CODMES] / 100),
   "MesNum", MOD([CODMES], 100),
   "FechaMes", DATE(INT([CODMES] / 100), MOD([CODMES], 100), 1),
   "MesTxt", FORMAT(DATE(INT([CODMES] / 100), MOD([CODMES], 100), 1), "YYYYMM"),
   "MesNombre", FORMAT(DATE(INT([CODMES] / 100), MOD([CODMES], 100), 1), "MMM YYYY"),
   "MesIndex", INT([CODMES] / 100) * 12 + MOD([CODMES], 100)
)


RELACION WEEKLY[CODMES] - DIMMES[CODMES] - DE MUCHOS A UNO EN ESE ORDEN
