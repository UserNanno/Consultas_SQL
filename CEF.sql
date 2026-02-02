En mi pagina V_GENERAL tengo un Grafico de columnas apiladas y lineas donde en eje X está CODMES de la tabla DIMMES. Asimismo, en el eje Y estan las dos medidas que estan en mi tabla MEDIDAS:
CTDSOLICITUDESAPROBADAS = SUM(WEEKLY[CTDSOLICITUDESAPROBADAS])
CTDSOLICITUDESDENEGADAS = SUM(WEEKLY[CTDSOLICITUDESDENEGADAS])
en el campo del eje Y de Linea está PASS_RATE_UND = DIVIDE([CTDSOLICITUDESAPROBADAS],[CTDSOLICITUDES]) que también pertenece a mi tabla medidas
Aqui mismo en este grafico hacemos y agregamos en información sobre herramientas a la pagina ZOOM_CEF
En mi pagina ZOOM_CEF tengo el grafico de Grafico de columnas apiladas donde en el eje X tengo el campo CODMES de la tabla DIMMES_AXIS, en el Eje Y tengo la medida FLGVENTANA_CTDSOLICITUDES de mi tabla medidas:
FLGVENTANA_CTDSOLICITUDES = 
VAR HoverCODMES  = SELECTEDVALUE(DIMMES[CODMES_NUM])
VAR HoverIndex   = LOOKUPVALUE(DIMMES[MESINDEX], DIMMES[CODMES_NUM], HoverCODMES)
VAR AxisCODMES   = SELECTEDVALUE(DIMMES_AXIS[CODMES])
VAR AxisIndex    = SELECTEDVALUE(DIMMES_AXIS[MesIndex])
RETURN
IF(
   NOT ISBLANK(HoverIndex)
&& AxisIndex >= HoverIndex - 3
&& AxisIndex <= HoverIndex,
   CALCULATE(
       [CTDSOLICITUDES],
       REMOVEFILTERS(DIMMES),
       REMOVEFILTERS(WEEKLY[CENTROATENCION]),
       KEEPFILTERS( TREATAS({AxisCODMES}, DIMMES[CODMES_NUM]) )
   ),
   BLANK()
)
En leyenda de este mismo grafico esta la dimensión Producto de Weekly. Y esta hoja esta activado el permitir uso como herramienta de información

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


DIMMES_AXIS = DIMMES

Considerar que la relación de ambos CODMES entre las tablas de WEEKLY y DIMMES esta WEEKLY[CODMES] + -> 1 DIMMES[CODMES] (ambos son texto)
