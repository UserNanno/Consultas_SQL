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
    "nodeSpacing": 50,
    "rankSpacing": 70
  }
}}%%

flowchart LR

classDef src fill:#E8F0FE,stroke:#1A73E8,stroke-width:2px,color:#0B1F44;
classDef stg fill:#E8F5E9,stroke:#1B5E20,stroke-width:2px,color:#0B1F44;
classDef key fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#0B1F44;
classDef out fill:#FFF7E6,stroke:#E65100,stroke-width:3px,color:#0B1F44;
classDef note fill:#F3E5F5,stroke:#6A1B9A,stroke-width:2px,color:#0B1F44,stroke-dasharray: 4 4;
classDef pad fill:transparent,stroke:transparent,color:transparent;

subgraph S0["CAPA 2 - PIPELINE TECNICO (detallado) · ENTRADAS"]
direction TB
S0PAD[" "]:::pad
A["Organico 1n_Activos (CSV)<br/>CODMES · matricula · superior<br/>nombre historico por mes"]:::src
B["Salesforce Estados (CSV)<br/>eventos por solicitud<br/>paso, estado, proceso, actores"]:::src
C["Salesforce Productos (CSV)<br/>registros por solicitud<br/>producto, etapa, divisa, montos<br/>analista y asignado"]:::src
D["PowerApps EDV (CSV)<br/>registros por solicitud<br/>matricula, resultado, motivos<br/>timestamp de creacion"]:::src
end

subgraph S1["1) CARGA Y NORMALIZACION BASE"]
direction TB
S1PAD[" "]:::pad
E1["Normalizacion de texto (norm_txt)<br/>upper + sin tildes<br/>espacios y caracteres raros"]:::stg
E2["Limpieza CESADO (limpiar_cesado)<br/>remueve '(CESADO)'<br/>solo para matching"]:::stg
E3["Parse fechas/horas (parse_fecha_hora_esp)<br/>dd/MM/yyyy hh:mm a<br/>normaliza AM/PM"]:::stg
E4["Montos a decimal(18,2)<br/>to_decimal_monto<br/>maneja separadores , y ."]:::stg
end

subgraph S2["2) STAGING POR FUENTE"]
direction TB
S2PAD[" "]:::pad
A1["Organico staging<br/>CSV ';' ISO-8859-1<br/>normaliza matricula/superior<br/>superior: quita 0 inicial"]:::key
B1["Estados staging<br/>parse timestamps inicio/fin<br/>deriva fecha + hora<br/>deriva mes de evaluacion"]:::key
C1["Productos staging<br/>filtra solicitudes validas (O00 + 11)<br/>parse fecha creacion (multi-formato)<br/>normaliza textos y montos"]:::key
D1["PowerApps staging<br/>normaliza textos<br/>parse timestamp creacion<br/>dedup: ultimo registro por solicitud"]:::key
end

subgraph S3["3) MATCH A ORGANICO (matricula y superior)"]
direction TB
S3PAD[" "]:::pad
T1["Tokens Organico<br/>split nombre limpio<br/>explode TOKEN por (mes, matricula)"]:::stg
T2["Tokens Salesforce<br/>split nombre actor/analista<br/>explode TOKEN por solicitud"]:::stg
M1["Matching por mes + token<br/>score: tokens_match >= 3<br/>ratio_sf >= 0.60<br/>desempate deterministico"]:::key
N0["Nota: nombres no se persisten<br/>solo uso transitorio para match"]:::note
end

subgraph S4["4) ENRIQUECIMIENTO CON ORGANICO"]
direction TB
S4PAD[" "]:::pad
B2["Estados enriquecidos<br/>match a matricula (actor)<br/>y matricula (actor del paso)<br/>+ superiores asociados"]:::key
C2["Productos enriquecidos<br/>match a matricula (analista)<br/>y matricula (analista asignado)<br/>(por mes de creacion)"]:::key
end

subgraph S5["5) SNAPSHOTS (N -> 1 por solicitud)"]
direction TB
S5PAD[" "]:::pad
S51["Snapshot ultimo estado<br/>order by inicio desc, fin desc<br/>conserva estado/proceso/paso<br/>+ mes evaluacion"]:::key
S52["Snapshot ultimo producto<br/>order by fecha creacion desc<br/>conserva producto/etapa/divisa<br/>+ montos + timestamps"]:::key
end

subgraph S6["6) DERIVADAS CLAVE"]
direction TB
S6PAD[" "]:::pad
D6a["Decision del analista<br/>filtra 'paso analista' por proceso<br/>TC: aprobacion analista<br/>CEF: evaluacion solicitud"]:::stg
D6b["Prioridad decision<br/>APROB/RECH (ultima)<br/>si no: RECUPERADA<br/>si no: PENDIENTE"]:::stg
D6c["Atribucion matricula analista<br/>MAT1 actor > MAT2 actor paso<br/>MAT3 prod analista > MAT4 asignado<br/>excluye roles sup/ger<br/>origen + TS base"]:::key
D6d["Tipo de producto (TC/CEF)<br/>derivado desde proceso"]:::stg
end

subgraph S7["7) ENSAMBLE FINAL + POWERAPPS"]
direction TB
S7PAD[" "]:::pad
F1["Ensamble base (ultimo estado)<br/>join atribucion analista<br/>join snapshot producto<br/>join decision analista"]:::key
F2["Lookup superior (1ra pasada)<br/>mes evaluacion + matricula analista<br/>coalesce superior"]:::stg
F3["PowerApps fallback/override<br/>override si difiere matricula<br/>si no: rellena nulos<br/>trae motivos + resultado"]:::key
F4["Lookup superior (2da pasada)<br/>por si cambio matricula"]:::stg
end

subgraph S8["8) SALIDA"]
direction TB
S8PAD[" "]:::pad
O1["Seleccion final de campos<br/>sin autonomias<br/>con trazabilidad y motivos"]:::stg
O2["Write Delta overwrite<br/>saveAsTable TP_SOLICITUDES_CENTRALIZADO"]:::out
end

A --> A1
B --> B1
C --> C1
D --> D1

A1 --> T1 --> M1
B1 --> T2 --> M1
C1 --> T2 --> M1
N0 -.-> M1

M1 --> B2
M1 --> C2

B2 --> S51
C2 --> S52

B2 --> D6a --> D6b
S51 --> D6c
S52 --> D6c
S51 --> D6d

S51 --> F1
S52 --> F1
D6b --> F1
D6c --> F1
D6d --> F1

F1 --> F2 --> F3 --> F4 --> O1 --> O2
D1 --> F3
