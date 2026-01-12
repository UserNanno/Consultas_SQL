Actúa como un asistente experto en extracción de datos de documentos y transformación de datos financieros y reportes EQUIFAX.
En base al PDF adjunto, extrae la información en un formato JSON que puedas entender, el cual debe contener información de deudas DIRECTAS de EQUIFAX, con glosas de producto, montos y fechas.
Posterior a la extracción, quiero que resumas los datos extraídos en una tabla final.

INSTRUCTION
Deberás seguir solo las PAUTAS_DE_NEGOCIO que detallo a continuación.

PAUTAS_DE_NEGOCIO

- Buscar y consolidar información únicamente de las tablas tituladas ENTIDAD – PARTE 1, ENTIDAD – PARTE 2, ENTIDAD – PARTE 3, etc., sin omitir ninguna de ellas y considerando únicamente los montos DIRECTOS de esas tablas, descartando indirectos, intereses, rendimientos, garantías u otras obligaciones.
- Solo debes considerar deudas DIRECTAS cuya glosa principal coincida exactamente con cualquiera de las siguientes:
  - "CREDITOS A MEDIANAS EMPRESAS"
  - "CREDITOS A PEQUENAS EMPRESAS"
  - "CREDITOS A GRANDES EMPRESAS"
- En caso existan más de una glosa principal en el mismo periodo, deberás sumarlas en una sola que contabilice para ese periodo.
- Dentro de esas glosas, solo considera los productos cuyas glosas sean exactamente:
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
- Considerando el punto anterior, toma en cuenta los importes de las deudas DIRECTAS de los últimos 3 años completos (por ejemplo, 31/12/2022, 31/12/2023, 31/12/2024) y las deudas de noviembre 2025 (MES DESEADO). Si algún periodo no tiene desglose por producto, consigna “0” para todos los productos en ese periodo.
- Cada columna corresponde a una fecha/período específico indicado en la instrucción anterior.
- En cada fila colocarás los importes de las deudas que correspondan a cada producto/tipo de deuda y período, respetando el orden de filas y columnas indicadas.
- Si algún producto de la lista no aparece en el documento para un periodo, debe registrarse como “0” en la tabla.
- En la última fila deberás registrar la suma total de todos los valores por columna, ya redondeados.
- En las tablas de productos, cuando un producto tenga valores en ambas columnas “S/” y “U$S” para un mismo periodo, debes SUMAR ambos valores para ese producto y periodo, ya que ambos están expresados en soles. No los trates como monedas distintas ni los omitas. Esta suma debe hacerse antes de cualquier redondeo.
- Ejemplo: Si para “CUOTAFIJA” en 31/12/2023 aparecen S/ 1,721,335.40 y U$S 370,900.03, el valor a reportar para ese producto y periodo será 1,721,335.40 + 370,900.03 = 2,092,235.43.
- En caso los valores salgan en decimales, redondéalos a miles usando la regla “half up ≥ 500”:
  - Si los últimos tres dígitos son ≥ 500, redondea hacia arriba.
  - Si son < 500, redondea hacia abajo.
  - Si el valor es menor a 1,000, solo sube a 1,000 si es ≥ 500; si no, queda en 0.
  - Ejemplos:
    - 3,401 → 3,000
    - 3,600 → 4,000
    - 450 → 0
  - El resultado final debe presentarse en miles, sin los tres últimos ceros.
  - Ejemplo:
    - 3,401 → 3
    - 3,600 → 4
    - 450 → 0
- El JSON de extracción debe mostrar los valores antes del redondeo y la tabla final solo los valores ya redondeados y en miles.
- En caso exista algún producto que no esté en la lista, agrégalo en una tabla posterior para tenerlo en cuenta con la misma estructura presentada.
- No agregues filas, columnas ni texto adicional fuera del contenido indicado. El resultado final debe ser una tabla, no un CSV.

Estructura de la tabla final (ejemplo):

DIRECTA , 31/12/{AÑO_1} , 31/12/{AÑO_2} , 31/12/{AÑO_3} , 30/{MES_DESEADO}/{AÑO_ACTUAL}
TARJCRED , {VALOR} , {VALOR} , {VALOR} , {VALOR}
AVCTACTE , {VALOR} , {VALOR} , {VALOR} , {VALOR}
SOBCTACTE , {VALOR} , {VALOR} , {VALOR} , {VALOR}
CREDXCOMEXT , {VALOR} , {VALOR} , {VALOR} , {VALOR}
REVOLVENTE , {VALOR} , {VALOR} , {VALOR} , {VALOR}
CUOTAFIJA , {VALOR} , {VALOR} , {VALOR} , {VALOR}
DESCUENTOS , {VALOR} , {VALOR} , {VALOR} , {VALOR}
LSBACK , {VALOR} , {VALOR} , {VALOR} , {VALOR}
ARRENDFIN , {VALOR} , {VALOR} , {VALOR} , {VALOR}
REPROGRAMADO , {VALOR} , {VALOR} , {VALOR} , {VALOR}
REFINANCIADO , {VALOR} , {VALOR} , {VALOR} , {VALOR}
BIENINMGENREN , {VALOR} , {VALOR} , {VALOR} , {VALOR}
FACTORING , {VALOR} , {VALOR} , {VALOR} , {VALOR}
INMOBILIARIO , {VALOR} , {VALOR} , {VALOR} , {VALOR}
TOTAL DE DEUDA EQUIFAX , {VALOR} , {VALOR} , {VALOR} , {VALOR}
