%md
## Objetivo del pipeline  
### Consolidación de solicitudes de crédito (última foto)

Este pipeline construye una tabla consolidada con granularidad **1 fila por CODSOLICITUD**, que representa la **última foto del estado de cada solicitud de crédito**, orientada a tableros de seguimiento operativo y de desempeño.

La tabla final integra información de **estado, producto, atribución y calidad**, resolviendo inconsistencias propias del flujo operativo y eliminando la dependencia de procesos manuales.

**Fuente base (universo):**
- `INFORMES_ESTADOS_*` (Salesforce), ya que contiene el tracking real del flujo de evaluación por pasos.

**Regla de oro:**
- El registro con la **mayor FECHORINICIOEVALUACION (timestamp)** determina el estado vigente de la solicitud:
  - PROCESO  
  - ESTADOSOLICITUD  
  - ESTADOSOLICITUDPASO  

**Fuentes de enriquecimiento:**
- `INFORME_PRODUCTO_*`: atributos del producto y montos (1 registro por solicitud).
- `POWERAPP_EDV.csv`: fuente complementaria utilizada como **COALESCE** y para captura de motivos y calidad.

**Estructura lógica de la tabla final:**

- **Identidad / tiempo**
  - CODSOLICITUD  
  - CODMESEVALUACION  
  - FECINICIOEVALUACION  
  - FECFINEVALUACION  

- **Estado actual de la solicitud**
  - PROCESO  
  - ESTADOSOLICITUD  
  - ESTADOSOLICITUDPASO  

- **Detalle del producto (Salesforce Productos)**
  - NBRPRODUCTO  
  - ETAPA  
  - TIPACCION  
  - NBRDIVISA  
  - MTOSOLICITADO  
  - MTOAPROBADO  
  - MTOOFERTADO  
  - MTODESEMBOLSADO  

- **Calidad y motivos (PowerApps principalmente)**
  - MOTIVORESULTADOANALISTA  
  - MOTIVOMALADERIVACION  
  - SUBMOTIVOMALADERIVACION  

- **Atribución**
  - MATANALISTA_FINAL  
  - ORIGEN_MATANALISTA  
  - MATSUPERIOR  

- **Autonomía** *(fuera del alcance en esta versión del pipeline base)*
  - FLGAUTONOMIA  
  - NIVELAUTONOMIA  
  - MATAUTONOMIA  

**Restricción de datos (DAC):**
- No se persisten nombres de personas.
- Los nombres se utilizan únicamente de forma transitoria para mapear contra el orgánico y obtener **matrículas**.











%md
## 7) Snapshots (1 fila por CODSOLICITUD)  
### 7.1 Estados: “último evento” define estado vigente

A partir de `INFORMES_ESTADOS_*`, se consolida la información a **1 fila por CODSOLICITUD**.

Criterio de selección:
- Se ordenan los registros por **FECHORINICIOEVALUACION (timestamp)** descendente.
- En caso de empate, se utiliza **FECHORFINEVALUACION** como desempate.

El registro más reciente determina:
- PROCESO  
- ESTADOSOLICITUD  
- ESTADOSOLICITUDPASO  
- CODMESEVALUACION  

Este snapshot representa el **estado actual de la solicitud**.










%md
## 7.2 Productos: 1 registro por CODSOLICITUD  
### Selección del producto vigente

A partir de `INFORME_PRODUCTO_*`, se consolida la información de producto a **1 fila por CODSOLICITUD**.

Criterio de selección:
- Se ordena por **FECCREACION** descendente.
- Se selecciona el registro más reciente por solicitud.

Este snapshot aporta:
- Detalle del producto  
- Etapa y tipo de acción  
- Divisa  
- Montos solicitados, aprobados, ofertados y desembolsados  

El resultado se utiliza exclusivamente como **enriquecimiento** del snapshot de estados.
