Estoy trasladando mis excel a databricks y estoy armando una query que sigue una logica de excel.

Estas son las columnas del excel
numerooportunidad	a.tipclasifinternasolicitud	a.destipclasifinternasolicitud	fechorsolicitud	a.destipestadosolicitud	a.destipestadojustificacionsolicitud	a.tipestadojustificacionsolicitud	a.codproducto	a.nbrmoneda	a.mtoaprobado	a.mtosolicitado	a.ctdplazoaprobado	matriculaanalista	codanalista	codanalistaaprobador	a.destipsolicitud	a.codmatriculaexceptuador	a.codexceptuador	fechoriniciocalif	fechorfincalif	a.codmatriculavendedor	a.nbrtipexcep	a.destipventacda	resultadoanalista	resultadocda	tipocampana	campana	a.fecdesembolso	c.tiprenta	a.descampania	a.destipevaluacionriesgo	f.destipetapa	c.bcp_cic__c	NOMBREmatriculaanalista	Areaanalista	NOMBREcodmatriculavendedor	AreaVendedor	TipoEvaluación	Estado_resultadoanalista	Estado_Manual	Analista_Manual	MatAnalista_Manual	Montodesembolsado	Campaña_hue	FecHor_iniciocalif	FecHor_fincalif	ResultadoAnalista_Manual		Estado resgistro Manual	.	.	Estado DataLake	.	.	Estado DataLake	.	.	Moneda	.	TipoCambio$	.	.	ScoreEvaluacion	SegmentoTitular	Cliente_PDH	destipevaluacionsolicitud			

Ahora, de todas estas, algunas tienen formula

NOMBREmatriculaanalista
=SI.ERROR(BUSCARV([@matriculaanalista];Organico!A:M;2;0);"NULL")

Areaanalista
=SI.ERROR(BUSCARV([@matriculaanalista];Organico!A:M;4;0);"NULL")

NOMBREcodmatriculavendedor
=SI.ERROR(BUSCARV([@[a.codmatriculavendedor]];Organico!A:O;2;0);"NULL")

AreaVendedor
=SI.ERROR(BUSCARV([@[a.codmatriculavendedor]];Organico!A:O;4;0);"NULL")

TipoEvaluación
=SI(BUSCARX(@A:A;Registro_Manual!A:A;Registro_Manual!A:A;"NULL";0;1)<>"NULL";"EVALUACION EN CENTRALIZADO";"EVALUACION AUTOMATICA")

Estado_resultadoanalista
=SI([@TipoEvaluación]="EVALUACION AUTOMATICA";BUSCARV([@[a.destipestadosolicitud]];$BC$2:$BD$18;2;0);
SI(Y([@TipoEvaluación]="EVALUACION EN CENTRALIZADO";[@[f.destipetapa]]="Desestimada";[@resultadoanalista]<>"Aprobado por Analista de créditos";[@resultadoanalista]<>"Aprobado por Gerente - Firmas y Desembolso");"SOLICITUD CANCELADA";
SI(Y([@TipoEvaluación]="EVALUACION EN CENTRALIZADO";[@resultadoanalista]="NULL");BUSCARV([@[a.destipestadosolicitud]];$BC$2:$BD$18;2;0);
SI([@TipoEvaluación]="EVALUACION EN CENTRALIZADO";BUSCARV([@resultadoanalista];$AZ$2:$BA$15;2;0);
[@TipoEvaluación]))))

Estado_Manual
=SI.ERROR(BUSCARV(BUSCARV([@numerooportunidad];Registro_Manual!A:P;6;0);$AW$2:$AX$12;2;0);
SI([@TipoEvaluación]="EVALUACION EN CENTRALIZADO";
[@[Estado_resultadoanalista]];
BUSCARV(BUSCARV([@numerooportunidad];Registro_Manual!A:P;6;0);$AW$2:$AX$12;2;0)
))

Analista_Manual
=SI.ERROR(SI.ERROR(BUSCARV(BUSCARV([@numerooportunidad];Registro_Manual!A:P;16;0);Organico_Manual!A:C;3;0);
SI(Y([@TipoEvaluación]="EVALUACION EN CENTRALIZADO";O([@[Estado_resultadoanalista]]="ACEPTADAS";[@[Estado_resultadoanalista]]="DENEGADAS";[@[Estado_resultadoanalista]]="OTRO ESTADO"));
[@NOMBREmatriculaanalista];
BUSCARV(BUSCARV([@numerooportunidad];Registro_Manual!A:P;16;0);Organico_Manual!A:C;3;0)
));[@NOMBREmatriculaanalista])

MatAnalista_Manual
=BUSCARV([@[Analista_Manual]];Organico_Manual!A:B;2;0)

Montodesembolsado
=SI([@[a.fecdesembolso]]="NULL";0;[@[a.mtoaprobado]])

Campaña_hue
=SI(O([@tipocampana]="NULL";[@tipocampana]="INF. NO DISP.");[@campana];[@tipocampana])

FecHor_iniciocalif
=SI([@fechoriniciocalif]="NULL";"NULL";[@fechoriniciocalif]-NSHORA(0;0;0))

FecHor_fincalif
=SI([@fechorfincalif]="NULL";"NULL";[@fechorfincalif]-NSHORA(0;0;0))

ResultadoAnalista_Manual
=BUSCARV([@numerooportunidad];Registro_Manual!A:F;6;0)

Rango AW:AX
Estado resgistro Manual	.
#N/D	#N/D
Denegado	DENEGADAS
Aprobado	ACEPTADAS
FACA	FACA
Desestimado	DESACTIVADA
ACEPTADA	ACEPTADAS
Denegado por Analista de credito	DENEGADAS
Aprobado por Analista de credito	ACEPTADAS
EN PROCESO	EN PROCESO
SOLICITUD CANCELADA	SOLICITUD CANCELADA
ENVIADO AL CENTRALIZADO	ENVIADO AL CENTRALIZADO
Aprobado por Analista de credito	ACEPTADAS

RANGO AZ:BA
Estado DataLake	.
Aprobado por Analista de crÃ©ditos	ACEPTADAS
Rechazado por Gerente - DocumentaciÃ³n Adicional	DENEGADAS
Aprobado por Gerente - Firmas y Desembolso	ACEPTADAS
Rechazado por Gerente - Firmas y Desembolso	DENEGADAS
Enviado a Analista de crÃ©ditos	EN PROCESO - SALES
Enviado a Gerente - Firmas y Desembolso	EN PROCESO - SALES	
Aprobado por Analista de créditos	ACEPTADAS
Rechazado por Gerente - Documentación Adicional	DENEGADAS
Enviado a Analista de créditos	EN PROCESO - SALES
Enviado a Gerente - Documentación Adicional	EN PROCESO - SALES


RANGO BC:BD
Estado DataLake	.
SOLICITUD CANCELADA	SOLICITUD CANCELADA - SALES
DESACTIVADA	DESACTIVADA - SALES
DENEGADA	DENEGADAS
ACEPTADA	ACEPTADAS
EN PROCESO	EN PROCESO - SALES
NULL	EN PROCESO - SALES	
Aprobado	ACEPTADAS
FACA	EN PROCESO - SALES
Desestimado	DESACTIVADA - SALES
DENEGADAS	DENEGADAS


La hoja Registor_Manual tiene estos campos
Title	FechaAsignacion	Tipo de Producto	TipoCampaña	TipoOferta	ResultadoAnalista	ResultadoCDA	BCI	Documentos	Detalle de documentos	PDH	Motivo Derivación - Centralizado	Score App	Comentarios	Campaña	Analista	Motivo_Digitado	dca-dene	Año	Mes	Creado	Modificado	Creado por	Motivo Resultado Analista	Motivo Derivación CDA	Motivo_Derivación	Modulo_Evaluación	Largo	10	O000274084	.	¿Se aprobó con Excepción?	¿Qué excepción se dio?	¿Adjuntaron documentos?	MotivoForzado	Moneda	Monto Aprobado	Segmento_Cliente	AnoRegistro	AnoRegistro: FechaAsignacion	Estado Civil	Ingreso 1era Titular	Ingreso 2da Titular	Ingreso 4ta Titular	Ingreso 5ta Titular	RUC empleador del Titular 5ta	Ingreso 1ra Conyuge	Ingreso 2da Conyuge	Ingreso 4ta Conyuge	Ingreso 5ta Conyuge	RUC empleador del conyuge 5ta	% Participación Titular	RUC del empleador Titular 4ta	RUC del empleador Conyuge 4ta	Evaluaciones sin Renta de 3ra	Evaluaciones Renta 3ra	Ingreso 3ra Titular	Ingreso 3ra Conyuge	% Participación Conyuge	¿Oportunidad mal derivada?	¿Empresa cliente bcp?	RUC de la empresa 3ra	Tipo de empresa	% Participación accionista 2	% Participación accionista 3	% Participación accionista 4	% Participación accionista 5	% Participación accionista 6	¿Oportunidad Mal derivada? VEHI	Categoría Mal Derivada	Adjunta_sustento_Ingresos	Mala_Derivacion_SiNo	TipoSustento	TipoSustento 1ra T	TipoSustento 1ra C	TipoSustento 2da T	TipoSustento 2da C	TipoSustento 3ra T	TipoSustento 3ra C	TipoSustento 4ta T	TipoSustento 4ta C	TipoSustento 5ta T	TipoSustento 5ta C	Tipo de elemento	Ruta de acceso	Largo	Duplicado	Cef	Tc	Nombre a Reemplazar	Resultado Analista- bricks	Resultado Analista-AP	op

Y esta sería la query que arme para replicar su logica. Me falta? que esta bien? que está mal?

WITH 
TP_BASE AS (
  SELECT
  A.CODSOLICITUD,
  A.TIPCLASIFINTERNASOLICITUD,
  A.DESTIPCLASIFINTERNASOLICITUD,
  A.FECSOLICITUD,
  A.HORSOLICITUD,
  A.DESTIPESTADOSOLICITUD,
  A.TIPESTADOJUSTIFICACIONSOLICITUD,
  A.DESTIPESTADOJUSTIFICACIONSOLICITUD,
  A.CODPRODUCTO,
  A.NBRMONEDA,
  A.MTOAPROBADO,
  A.MTOSOLICITADO,
  A.CTDPLAZOSOLICITADO,
  A.CODCOLABORADORANALISTA,
  A.CODMATRICULACOLABORADORANALISTA,
  A.CODCOLABORADORAPROBADOR,
  A.CODMATRICULACOLABORADORAPROBADOR,
  A.CODCOLABORADOREXCEPTUADOR,
  A.CODMATRICULACOLABORADOREXCEPTUADOR,
  A.CODCOLABORADORVENDEDOR,
  A.CODMATRICULACOLABORADORVENDEDOR,
  A.DESTIPSOLICITUD,
  A.FECINICIOCALIF,
  A.FECFINCALIF,
  A.NBREXCEP,
  A.NBRTIPEXCEP,
  A.DESTIPVENTACDA,
  A.DESTIPESTADOEVALUACIONANALISTA AS RESULTADOANALISTA,
  A.DESTIPESTADOEVALUACIONCDA,
  A.DESTIPEVALUACIONRIESGO,
  A.DESCAMPANIA,
  A.FECDESEMBOLSO,
  A.TIPETAPA,
  A.DESTIPETAPA,
  A.CODINTERNOCOMPUTACIONAL,
  B.AREA_TRIBU_COE AS AREAANALISTA,
  C.AREA_TRIBU_COE AS AREAVENDEDOR,
  A.DESTIPEVALUACIONSOLICITUD,
  (CASE
    WHEN D.OPERACION IS NOT NULL THEN 'EVALUACION EN CENTRALIZADO'
    ELSE 'EVALUACION AUTOMATICA'
  END) AS TIPEVALUACIONMANUAL,
  D.MATANALISTA AS MATANALISTAMANUAL,
  (CASE
    WHEN A.FECDESEMBOLSO IS NULL THEN 0
    ELSE A.MTOAPROBADO
  END) AS MTODESEMBOLSADOMANUAL,
  (CASE
    WHEN A.DESTIPEVALUACIONRIESGO IS NULL OR A.DESTIPEVALUACIONRIESGO = 'INF. NO DISP.' THEN A.DESCAMPANIA
    ELSE A.DESTIPEVALUACIONRIESGO
  END) AS CAMPANIAHUEMANUAL,
  D.RESULTADOANALISTA AS RESULTADOANALISTAMNUAL
FROM CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.M_SOLICITUDCREDITOCONSUMO A
LEFT JOIN CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.T72496_ORGANICO B ON (A.CODMATRICULACOLABORADORANALISTA = B.MATORGANICO AND B.CODMES = TO_CHAR(A.FECSOLICITUD, 'yyyyMM'))
LEFT JOIN CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.T72496_ORGANICO C ON (A.CODMATRICULACOLABORADORVENDEDOR = C.MATORGANICO AND C.CODMES = TO_CHAR(A.FECSOLICITUD, 'yyyyMM'))
LEFT JOIN CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.T72496_REGISTRO_OPORTUNIDADES D ON (A.CODSOLICITUD = D.OPERACION)
WHERE A.FECSOLICITUD >= DATE '2025-10-01' AND A.CODSOLICITUD = 'O0017847584'
)
SELECT
  A.*,
  (CASE 
    WHEN A.TIPEVALUACIONMANUAL = 'EVALUACION AUTOMATICA' THEN
      (CASE 
        WHEN A.DESTIPESTADOSOLICITUD = 'SOLICITUD CANCELADA' THEN 'SOLICITUD CANCELADA - SALES'
        WHEN A.DESTIPESTADOSOLICITUD = 'DESACTIVADA' THEN 'DESACTIVADA - SALES'
        WHEN A.DESTIPESTADOSOLICITUD = 'DENEGADA' THEN 'DENEGADAS'
        WHEN A.DESTIPESTADOSOLICITUD = 'ACEPTADA' THEN 'ACEPTADAS'
        WHEN A.DESTIPESTADOSOLICITUD = 'EN PROCESO' THEN 'EN PROCESO - SALES'
        WHEN A.DESTIPESTADOSOLICITUD IS NULL THEN 'EN PROCESO - SALES'
      END)
    WHEN A.TIPEVALUACIONMANUAL = 'EVALUACION EN CENTRALIZADO' AND A.DESTIPETAPA = 'DESESTIMADA' AND A.RESULTADOANALISTA NOT IN ('APROBADO POR ANALISTA DE CREDITOS', 'APROBADO POR GERENTE - FIRMAS Y DESEMBOLSO') THEN 'SOLICITUD CANCELADA'
    WHEN A.TIPEVALUACIONMANUAL = 'EVALUACION EN CENTRALIZADO' AND A.RESULTADOANALISTA IS NULL THEN
      (CASE 
        WHEN A.DESTIPESTADOSOLICITUD = 'SOLICITUD CANCELADA' THEN 'SOLICITUD CANCELADA - SALES'
        WHEN A.DESTIPESTADOSOLICITUD = 'DESACTIVADA' THEN 'DESACTIVADA - SALES'
        WHEN A.DESTIPESTADOSOLICITUD = 'DENEGADA' THEN 'DENEGADAS'
        WHEN A.DESTIPESTADOSOLICITUD = 'ACEPTADA' THEN 'ACEPTADAS'
        WHEN A.DESTIPESTADOSOLICITUD = 'EN PROCESO' THEN 'EN PROCESO - SALES'
        WHEN A.DESTIPESTADOSOLICITUD IS NULL THEN 'EN PROCESO - SALES'
      END)
    WHEN A.TIPEVALUACIONMANUAL = 'EVALUACION EN CENTRALIZADO' THEN
      (CASE
        WHEN A.RESULTADOANALISTA = 'APROBADO POR ANALISTA DE CREDITOS' THEN 'ACEPTADAS'
        WHEN A.RESULTADOANALISTA = 'APROBADO POR GERENTE - FIRMAS Y DESEMBOLSO' THEN 'ACEPTADAS'
        WHEN A.RESULTADOANALISTA = 'RECHAZADO POR GERENTE - DOCUMENTACION ADICIONAL' THEN 'DENEGADAS'
        WHEN A.RESULTADOANALISTA = 'RECHAZADO POR GERENTE - FIRMAS Y DESEMBOLSO' THEN 'DENEGADAS'
        WHEN A.RESULTADOANALISTA = 'ENVIADO A ANALISTA DE CREDITOS' THEN 'EN PROCESO - SALES'
        WHEN A.RESULTADOANALISTA = 'ENVIADO A GERENTE - DOCUMENTACION ADICIONAL' THEN 'EN PROCESO - SALES'
        WHEN A.RESULTADOANALISTA = 'ENVIADO A GERENTE - FIRMAS Y DESEMBOLSO' THEN 'EN PROCESO - SALES'
      END)
    ELSE A.TIPEVALUACIONMANUAL
  END) AS ESTADORESULTADOANALISTAMANUAL
FROM TP_BASE A;



CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.M_SOLICITUDCREDITOCONSUMO tabla principal
CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.T72496_ORGANICO SIMIL A LA HOJA ORGANICO DE EXCEL
CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.T72496_REGISTRO_OPORTUNIDADES SIMIL A LA HOJA DE REGISTRO MANUAL DE EXCEL

TIPOCAMPANA ES DESTIPEVALUACIONRIESGO
LOS NOMBRES NO VAN POR SER DATOS DE ALTA CRITICIDAD
Estado_resultadoanalista DE EXCEL ES ESTADORESULTADOANALISTAMANUAL
Estado_Manual no se ha implementado en sql databricks
Analista_Manual no se ha implementado en sql databricks
las fechas (FecHor_iniciocalif	FecHor_fincalif) no van
AREA_TRIBU_COE es el area del analista y vendedor
