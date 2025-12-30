Actúa como un asistente experto en extracción de datos de documentos y transformación de datos financieros y reportes EQUIFAX

En base al PDF adjunto extrae la información en un formato JSON que puedas entender el cual debe contener información de deudas DIRECTAS de EQUIFAX, con glosas de producto, montos y fechas

Posterior a la extracción quiero que resumas los datos extraidos en una tabla final.

INSTRUCTION
Deberás seguir solo las PAUTAS_DE_NEGOCIO que detallo a continuación

PAUTAS_DE_NEGOCIO
1. Buscar y consolidar información únicamente de las tablas tituladas ENTIDAD – PARTE 1, ENTIDAD – PARTE 2, ENTIDAD – PARTE 3, etc., sin omitir ninguna de ellas y considerar únicamente los montos DIRECTOS de esas tablas, descartando indirectos, intereses, rendimientos, garantías u otras obligaciones.

2. Solo debes considerar deudas DIRECTAS cuya glosa principal coincida con cualquiera de las siguientes:
   - "CREDITOS A MEDIANAS EMPRESAS"
   - "CREDITOS A PEQUENAS EMPRESAS"
   - "CREDITOS A GRANDES EMPRESAS"

- En caso existan mas de 1 glosa principal deberás sumarlas en una sola que contabilice para ese periodo

3. Dentro de esas glosas, solo considera los productos cuyas glosas sean exactamente:
   - "TARJCRED"
   - "AVCTACTE"
   - "SOBCTACTE"
   - "CREDXCOMEXT"
   - "REVOLVENTE"
   - "CUOTAFIJA"
   - "LSBACK"
   - "DESCUENTOS"
   - "ARRENDFIN"
   - "REPROGRAMADO"
   - "REFINANCIADO"
   - "BIENINMGENREN"
   - "FACTORING"
   - "INMOBILIARIO"

4. Considerando el punto 2, vas a tomar en cuenta los importes de las deudas DIRECTAS de los ultimos 3 años y las deudas de noviembre 2025 (MES DESEADO). Considera que el año actual es 2025
   - Cada columna corresponde a una fecha/período específico indiciado en la instruccion anterior
   - En cada fila colocarás los importes de las deudas que correspondan a cada producto/tipo de deuda y período, respetando el orden de filas y columnas indicadas.
5. En la ultima fila deberás registrar la suma total de todos los valores por columna

Muestrame el resultado como una tabla siguiendo la estructura CSV que se presenta a continuación:

DIRECTA,31/12/{AÑO_1},31/12/{AÑO_2},31/12/{AÑO_3},30/{MES_DESEADO}/{AÑO_ACTUAL}
TARJCRED,{VALOR},{VALOR},{VALOR},{VALOR}
AVCTACTE,{VALOR},{VALOR},{VALOR},{VALOR}
SOBCTACTE,{VALOR},{VALOR},{VALOR},{VALOR}
CREDXCOMEXT,{VALOR},{VALOR},{VALOR},{VALOR}
REVOLVENTE,{VALOR},{VALOR},{VALOR},{VALOR}
CUOTA FIJA,{VALOR},{VALOR},{VALOR},{VALOR}
DESCUENTOS,{VALOR},{VALOR},{VALOR},{VALOR}
LSBACK,{VALOR},{VALOR},{VALOR},{VALOR}
ARRENDFIN,{VALOR},{VALOR},{VALOR},{VALOR}
REPROGRAMADO,{VALOR},{VALOR},{VALOR},{VALOR}
REFINANCIADO,{VALOR},{VALOR},{VALOR},{VALOR}
BIENINMGENREN,{VALOR},{VALOR},{VALOR},{VALOR}
FACTORING,{VALOR},{VALOR},{VALOR},{VALOR}
INMOBILIARIO,{VALOR},{VALOR},{VALOR},{VALOR}
TOTAL DE DEUDA EQUIFAX,{VALOR},{VALOR},{VALOR},{VALOR}

5. Toma en cuenta las siguientes indicaciones:
	- En caso los valores salgan en decimales, redondealos. Por ejemplo si es 376400 redondeado sería 376 y si es 376600 sería 377
	- En caso exista algún producto que no esté en la lista, agregalo en una tabla posterior para tenerlo en cuenta con la misma estructura presentada
	- En las tablas de los productos encontrarás columnas con encabezados de "S/" y "U$S". Esto no indica que el monto que figura está en esa moneda, el valor ya está convertido en soles. En caso exista para un mismo producto valores en ambas columnas, deberás sumarlas.
	- No agregues filas, columnas ni texto adicional fuera del contenido indicado
	- El resultado final debe ser una tabla no un csv
