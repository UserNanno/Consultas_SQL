%%{init: {
  "theme": "base",
  "themeVariables": {
    "fontFamily": "Inter, Segoe UI, Arial",
    "fontSize": "16px",
    "primaryColor": "#FFF7E6",
    "primaryTextColor": "#0B1F44",
    "primaryBorderColor": "#E65100",
    "lineColor": "#37474F",
    "tertiaryColor": "#F5F7FA"
  },
  "flowchart": {
    "curve": "basis",
    "nodeSpacing": 55,
    "rankSpacing": 75
  }
}}%%

flowchart LR

classDef src fill:#E8F0FE,stroke:#1A73E8,stroke-width:2px,color:#0B1F44;
classDef stg fill:#E8F5E9,stroke:#1B5E20,stroke-width:2px,color:#0B1F44;
classDef key fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#0B1F44;
classDef out fill:#FFF7E6,stroke:#E65100,stroke-width:3px,color:#0B1F44;
classDef note fill:#F3E5F5,stroke:#6A1B9A,stroke-width:2px,color:#0B1F44,stroke-dasharray: 4 4;
classDef pad fill:transparent,stroke:transparent,color:transparent;

%% =======================
%% 0) FUENTES
%% =======================
subgraph S0["CAPA NEGOCIO · FUENTES Y PARAMETROS"]
direction TB
S0PAD[" "]:::pad

SRC1["TP_SOLICITUDES_CENTRALIZADO<br/>1 fila por solicitud<br/>matricula, decision, motivos<br/>montos y divisa"]:::src
SRC2["H_COLABORADOR (Activo)<br/>estructura organizacional<br/>area/servicio + superior<br/>fecha dia + actualizacion"]:::src
SRC3["M_SOLICITUDCREDITOCONSUMO<br/>core solicitudes consumo<br/>timestams y estados origen"]:::src
SRC4["M_SOLICITUDTARJETACREDITO<br/>core solicitudes TC<br/>timestams y estados origen"]:::src
SRC5["M_SOLICITUDINDIVIDUO (Titular)<br/>segmento + tipo renta<br/>por solicitud"]:::src
SRC6["DIAS NO HABILES<br/>feriados<br/>para flag dia util"]:::src

P1["Parametro tipo de cambio<br/>TC_USD_SOL = 3.81<br/>(estatico)"]:::stg
end

%% =======================
%% 1) BASE CENTRALIZADO (Salesforce consolidado)
%% =======================
subgraph S1["1) BASE CENTRALIZADO (desde TP_SOLICITUDES_CENTRALIZADO)"]
direction TB
S1PAD[" "]:::pad

B1["Base Salesforce (en soles)<br/>convierte USD->SOL<br/>con TC_USD_SOL fijo"]:::key
B2["Campos clave negocio<br/>solicitud, mes evaluacion<br/>matricula analista<br/>estado de decision<br/>producto/etapa/motivos"]:::stg
B3["Regla cambio de tasa<br/>flag = 1 si<br/>'DEBIO APROBARSE EN PDC'"]:::stg
end

%% =======================
%% 2) DIMENSION ORGANICA (analista / vendedor)
%% =======================
subgraph S2["2) DIMENSION ORGANICA (H_COLABORADOR)"]
direction TB
S2PAD[" "]:::pad

D1["Filtrado base<br/>solo colaboradores Activos"]:::stg
D2["Derivacion CANAL<br/>DCA / TLMK / ENALTA / BEX<br/>segun area + servicio"]:::key
D3["Dedup diario por matricula<br/>ROW_NUMBER por FECDIA+MAT<br/>toma ultimo registro actualizado"]:::key

A_FB["Fallback organico Analista (-7 dias)<br/>match por matricula<br/>ventana: FECINICIOEVAL-7 a FECINICIOEVAL<br/>toma el FECDIA mas cercano"]:::key
V_FB["Fallback organico Vendedor (-7 dias)<br/>match por matricula vendedor<br/>ventana: FECSOL-7 a FECSOL<br/>toma el FECDIA mas cercano"]:::key
end

%% =======================
%% 3) CORE SOLICITUDES (consumo + TC)
%% =======================
subgraph S3["3) CORE SOLICITUDES (union consumo + tarjeta)"]
direction TB
S3PAD[" "]:::pad

C1["Consumo<br/>mapea producto (LD / CdD / otros)<br/>calcula timestamps<br/>convierte montos USD->SOL"]:::key
C2["Tarjeta<br/>clasifica tipo operacion (stock/nueva)<br/>normaliza campaña y riesgo<br/>convierte montos USD->SOL"]:::key
C3["Union ALL<br/>estructura comun<br/>(consumo + tarjeta)"]:::stg

C4["Campos operativos<br/>fecha/horas, semana del mes<br/>dia de semana + ordinal"]:::stg
end

%% =======================
%% 4) ENRIQUECIMIENTOS NEGOCIO
%% =======================
subgraph S4["4) ENRIQUECIMIENTOS NEGOCIO"]
direction TB
S4PAD[" "]:::pad

Sg1["Segmento + renta<br/>desde Individuo Titular<br/>mapa subsegmento a segmento<br/>mapa tiprenta a etiqueta"]:::stg

Cal1["Calendario + dia util<br/>genera rango 2024-01-01 a 2026-12-31<br/>domingo o feriado = 0<br/>si no = 1"]:::key

T1["Tiempos (horas)<br/>solicitud -> desembolso<br/>solicitud -> inicio evaluacion<br/>inicio -> fin evaluacion<br/>solicitud -> fin evaluacion"]:::key
end

%% =======================
%% 5) BASE UNIFICADA + REGLAS (centro y estado final)
%% =======================
subgraph S5["5) BASE UNIFICADA + REGLAS DE CLASIFICACION"]
direction TB
S5PAD[" "]:::pad

U1["Base Union enriquecida<br/>join vendedor fallback<br/>join segmento/renta<br/>calcula tiempos (horas)"]:::key

U2["Cruce con base Centralizado<br/>left join por solicitud<br/>trae matricula, decision, motivos<br/>prioriza montos si aplica"]:::key

R1["Regla Centro de Atencion<br/>CENTRALIZADO si:<br/>existe analista y area = Tribu Riesgos BDN<br/>PDC si:<br/>condiciones por tipoperacion + estado origen + area vendedor"]:::key

R2["Regla Estado final<br/>CENTRALIZADO: decision analista<br/>PDC: ACEPTADA->APROBADO<br/>PDC: DENEGADA/DESACTIVADA->RECHAZADO<br/>si no: coalesce"]:::key
end

%% =======================
%% 6) KPIS / FLAGS / BRACKETS
%% =======================
subgraph S6["6) KPIs, FLAGS Y CLASIFICADORES"]
direction TB
S6PAD[" "]:::pad

K1["KPIs de funnel<br/>aprobado / rechazado / recuperada<br/>desembolsado vs no desembolsado<br/>renegociado vs no renegociado"]:::key

K2["Montos por estado<br/>aprobado, rechazado, recuperada<br/>denegado total<br/>renegociado (delta)"]:::key

K3["Mala derivacion<br/>solo si RECHAZADO y hay motivo<br/>flag + monto asociado"]:::stg

K4["Autonomia (solo clasificador)<br/>analista <= 100k<br/>supervisor 100k-240k<br/>gerente > 240k"]:::stg

K5["Brackets de tiempos<br/>atencion solicitud<br/>derivacion a analista<br/>atencion analista<br/>atencion total"]:::stg

K6["Brackets renegociado<br/>0-10k, 10-20k, ... , >=50k"]:::stg
end

%% =======================
%% 7) SALIDA HM  (SIN NOTAS)
%% =======================
subgraph S7["7) SALIDA"]
direction TB
S7PAD[" "]:::pad

O1["Filtro final<br/>solo: CENTRALIZADO y PUNTO DE CONTACTO"]:::stg
O2["Tabla final HM_SOLICITUDES_SCRM<br/>Delta en ruta definida<br/>lista para tableros SCRM"]:::out
end

%% =======================
%% 8) SUPUESTOS / NOTAS (APARTE)
%% =======================
subgraph S8["SUPUESTOS / NOTAS"]
direction TB
S8PAD[" "]:::pad

N1["TC_USD_SOL fijo<br/>si cambia, se versiona o se parametriza"]:::note
N2["Fallback orgánico (-7 días)<br/>elige FECDIA más reciente dentro de la ventana"]:::note
end

%% =======================
%% FLUJO
%% =======================
P1 --> B1
SRC1 --> B1 --> B2
B2 --> B3

SRC2 --> D1 --> D2 --> D3
B2 --> A_FB
D3 --> A_FB
C3 --> V_FB
D3 --> V_FB

SRC3 --> C1 --> C3
SRC4 --> C2 --> C3
C3 --> C4

SRC5 --> Sg1
SRC6 --> Cal1
C3 --> T1

C3 --> U1
V_FB --> U1
Sg1 --> U1
T1 --> U1

B2 --> U2
U1 --> U2
A_FB --> U2
Cal1 --> U2

U2 --> R1 --> R2
R2 --> K1 --> K2 --> O1 --> O2
R2 --> K3 --> O1
K4 --> O1
K5 --> O1
K6 --> O1

N1 -.-> B1
N2 -.-> A_FB
N2 -.-> V_FB

