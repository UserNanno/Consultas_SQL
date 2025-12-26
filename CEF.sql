Actúa como un asistente experto en transformación de datos financieros y reportes SBS.

Las PALABRAS EN MAYÚSCULAS son mis marcadores de posición. Conserva el formato y la plantilla general que te proporciono. No agregues explicaciones fuera de la plantilla.

INSTRUCTION
Con el JSON anterior que proporcionaste, quiero que conviertas la respuesta en datos estructurados compatibles con el archivo Excel AJUSTES DEUDA EQUIFAX, usando su estructura real de hoja, filas y columnas como referencia, y siguiendo las PAUTAS_DE_NEGOCIO que detallo a continuación. La respuesta final debe ser un archivo lógico en formato CSV que replique esa hoja de Excel.

CONTEXT
- El JSON anterior que proporcionaste contiene información de deudas DIRECTAS de EQUIFAX, con glosas de producto, montos y fechas.
- Debo cargar esta información en el archivo Excel AJUSTES DEUDA EQUIFAX, en la hoja y rango donde se registran las deudas directas.
- El archivo AJUSTES DEUDA EQUIFAX ya tiene definida su estructura (encabezados, columnas y filas). Debes usar esa estructura como referencia para construir la salida en CSV.

PAUTAS_DE_NEGOCIO
1. Solo debes considerar deudas DIRECTAS cuya glosa principal sea:
   - "CREDITOS A MEDIANAS EMPRESAS"
   - "CREDITOS A PEQUEÑAS EMPRESAS"

2. Dentro de esas glosas, solo considera los productos cuyas glosas sean exactamente:
   - "TARJCRED"
   - "SOBCTACTE"
   - "CREDXCOMEXT"
   - "RESOLVENTE"
   - "CUOTA FIJA"
   - "DESCUENTOS"
   - "ARRENDFIN"
   - "REPROGRAMADO"
   - "REFINANCIADO"

3. Considerando el punto 2, vas a registrar en el Excel (rango B2 hasta E11) los importes de las deudas DIRECTAS según la fecha indicada en las celdas de encabezado desde B1 hasta E1:
   - Cada columna (B, C, D, E) corresponde a una fecha/período específico indicada en la fila 1.
   - En cada fila del rango B2:E11 colocarás los importes de las deudas que correspondan a cada producto/tipo de deuda y período, respetando el orden de filas y columnas del archivo AJUSTES DEUDA EQUIFAX.
4. En la celda B11 debes registrar la suma de todos los importes desde la celda B2 hasta B10. 
   - Usa una fórmula de suma de Excel equivalente a: =SUMA(B2:B10)
   - Copia esta misma lógica a las otras columnas en la fila 11, es decir: C11, D11 y E11 deben contener la suma de sus respectivos rangos (C2:C10, D2:D10, E2:E10).

FORMATO_SALIDA_CSV
- Usa el archivo Excel AJUSTES DEUDA EQUIFAX como referencia de formato para construir el CSV:
  - Mismos encabezados de la hoja.
  - Misma cantidad de columnas y el mismo orden.
  - Filas desde la 2 hasta la 11 llenas según las PAUTAS_DE_NEGOCIO.
- La salida debe ser un CSV que represente exactamente esa hoja de Excel, listo para guardar como archivo .csv.
- Usa coma como separador de campos (,) a menos que el contexto requiera punto y coma (;).
- Incluye en las celdas de la fila 11 las fórmulas de suma como texto de celda (por ejemplo, =SUMA(B2:B10)), para que al abrir el CSV en Excel se reconozcan como fórmulas.
- No agregues filas, columnas ni texto adicional fuera del contenido de la hoja.

ENTRADA
- El JSON que me proporcionaste con la información de deudas DIRECTAS de EQUIFAX.
- ARCHIVO_EXCEL_REFERENCIA: adjuntaré el archivo AJUSTES DEUDA EQUIFAX para que uses su estructura (hoja, encabezados, filas y columnas) como modelo.

PLANTILLA_DE_SALIDA
CONTENIDO_CSV
