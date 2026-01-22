Mira tengo esta query que me indica las solicitudes de credito consumo que tuvo un cliente en especifico (Codinternocomputacional)
	SELECT
		A.CODMESEVALUACION,
		(CASE
			WHEN A.DESCANALVENTARBMPER = 'SALEFORCE' THEN SUBSTR(TRIM(A.NUMSOLICITUDEVALUACION), 5, 7)
			WHEN A.DESCANALVENTARBMPER = 'LOANS' THEN SUBSTR(TRIM(A.NUMSOLICITUDEVALUACION), 3, 8)
			ELSE TRIM(A.NUMSOLICITUDEVALUACION)
		END) AS NUMSOLICITUDEVALUACIONCORTO,
		A.FECSOLICITUDEVALUACION,
		A.FECEVALUACION,
		A.DESCANALVENTARBMPER,
		A.TIPPRODUCTOSOLICITUDRBM,
		A.DESCANALVENTASOLICITUDRBM,
		A.DESTIPDECISIONRESULTADORBM,
		A.CODINTERNOCOMPUTACIONAL,
		A.DESTIPDECISIONRESULTADOPRIORIZACION,
		A.DESCAMPANIASOLICITUD,
		A.DESTIPEVALUACIONSOLICITUDCREDITO,
		B.CODCLAVEPARTYCLI
	FROM CATALOG_LHCL_PROD_BCP.BCP_DDV_RBMRBMPER_MODELOGESTION_VU.MD_EVALUACIONSOLICITUDCREDITO A
	LEFT JOIN CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.M_CLIENTE B ON (TRIM(A.CODINTERNOCOMPUTACIONAL) = TRIM(B.CODINTERNOCOMPUTACIONAL))
	WHERE A.TIPPRODUCTOSOLICITUDRBM = 'CC' AND A.DESCANALVENTARBMPER NOT IN ('YAPE', 'OTROS') AND TRIM(B.CODINTERNOCOMPUTACIONAL) = '23045191';

Por ejemplo para el 202301 hay dos registros (fecsolicitudevaluacion 2023-01.04)donde con esta query son identicos los registros pero son dos solicitudes distintas. Como lo verificamos con el campo A.CODIDVALUACION, también con A.CODEVALUACIONSOLICITUD, también con A.NUMSOLICITUDEVALUACION y también con el campo A.CODSECUENCIALDECISIONRBM

También para 202503 hay 12 registros que indican que hubo 12 solicitudes (va desde fecsolicitudevaluacion 2025-03-01 a 2025-03-23 donde 

También para 202508 hay 5 registros que indican que hubo 5 solicitudes (va desde fecsolicitudevaluacion 2025-08-26 a 2025-08-31

También para 202509 hay 6 registros que indican que hubo 6 solicitud (va desde fecsolicitudevaluacion 2025-09-02 hasta 2025-08-02)

También para 202510 hay 1 registro que indica que hubo 1 solicitud (fecsolicitudevaluacion 2025-10-27)

donde todas las solicitudes tienen DESTIPDECISIONRESULTADORBM como 'Approve' pero en 202510 sale 'Decline' el unico registro

Ahora tenemos otra tabla donde salen las ventas de credito consumo donde para el mismo cliente hago:

	SELECT
		CAST(date_format(FECAPERTURA, 'yyyyMM') AS INT) AS CODMESAPERTURA,
		CODSOLICITUD,
		(CASE
			WHEN SUBSTR(TRIM(CODSOLICITUD), 1, 2) IN ('CX', 'DX') THEN SUBSTR(TRIM(CODSOLICITUD), 3, 8) --LOANS
			WHEN CODSOLICITUD LIKE '2________' THEN SUBSTR(TRIM(CODSOLICITUD), 3, 7) -- SF
			WHEN CODSOLICITUD LIKE '      2________' THEN SUBSTR(TRIM(CODSOLICITUD), 3, 7) -- SF
			WHEN CODSOLICITUD LIKE 'O%' THEN SUBSTR(TRIM(CODSOLICITUD), 3, 7) -- SF
			ELSE TRIM(CODSOLICITUD) -- BMO / CUOTEALO
		END) AS CODSOLICITUDCORTO,
		(CASE
			WHEN CODPRODUCTO = 'CPEFIA' THEN 'CUOTEALO'
			WHEN SUBSTR(TRIM(CODSOLICITUD), 1, 2) = 'PE' THEN 'CUOTEALO'
			WHEN CODPRODUCTO = 'CPEYAP' THEN 'YAPE'
			WHEN SUBSTR(TRIM(CODSOLICITUD), 1, 2) = 'YP' THEN 'YAPE'
			WHEN SUBSTR(TRIM(CODSOLICITUD), 1, 2) = 'JB' THEN 'BANCA MÓVIL'
			WHEN SUBSTR(TRIM(CODSOLICITUD), 1, 2) IN ('CX', 'DX') THEN 'LOANS'
			WHEN CODSOLICITUD LIKE '2________' THEN 'SALEFORCE'
			WHEN CODSOLICITUD LIKE '      2________' THEN 'SALEFORCE'
			WHEN CODSOLICITUD LIKE 'O%' THEN 'SALEFORCE'
			ELSE 'OTROS'
		END) AS CANAL_MDPREST,
		FECAPERTURA,
		FECDESEMBOLSO,
		MTOSALDOCREDITOINICIOCAMPANIA,
		MTOORIGINALCREDITO,
		MTODESEMBOLSADO,
		MTODESEMBOLSADOCONTRAVALORSBSSOL,
		CODCLAVEPARTYCLI,
		FLGREGELIMINADOFUENTE
	FROM CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.M_CUENTACREDITOPERSONAL
	WHERE CODPRODUCTO IN ('CPEEFM','CPECMC','CPEDPP','CPECEM','CPEECV','CPEGEN','CPEADH','CPEFIA') AND FLGREGELIMINADOFUENTE = 'N'
	AND TRIM(CODCLAVEPARTYCLI) = '3d6d83bae02a3aea64015f05a8c46b1f7092c45a89b40f6a8bdd4f35c7a2b3891bce9b788d377015dad23223bbfc8f129cd621e9ef8fde4b3d679cc2d92936cc';


Donde podemos observar que hay 3 registros nada más.

En CODMESAPERTURA 202301 con CODSOLICITUD DX1672849595498 y CODSOLICITUDCORTO 16728495 vemos que por el canal de LOANS aperturó una cuenta en FECAPERTURA 2023-01-04 y se desembolso ese mismo dia un monto de 8800 que figura tanto en MTOORIGINALCREDITO, TODESEMBOLSADO, MTODESEMBOLSADOCONTRAVALORSBSSOL.
Para 202503 con CODSOLICITUD JB0001408663 y CODSOLICITUDCORTO JB0001408663 para el canal de BANCA MÓVIL con FECAPERTURA 2025-03-24 y tanto MTOORIGINALCREDITO, MTODESEMBOLSADO, MTODESEMBOLSADOCONTRAVALORSBSSOL ES 1500
Para 202509 con CODSOLICITUD JB0003779523 y CODSOLICITUDCORTO JB0003779523 para el canal de BANCA MÓVIL con FECAPERTURA 2025-09-08 y tanto MTOORIGINALCREDITO, MTODESEMBOLSADO, MTODESEMBOLSADOCONTRAVALORSBSSOL ES 1500


Quisiera tener como un consolidado de esto donde por ejemplo tengamos un FLG de si una solicitud de credito fue ingresada posterior a una. Es decir, si es por un reingreso. El tiempo es 1 mes calendario, es decir si una solicitud se dio el 13 de agosto solo esperarmos hasta el 13 de setiembre para considerar la nueva solicitud como reingreso, desde el 14 de setiembre ya no.
A esto agregarle los campos del desembolso que se tiene como FECAPERTURA, FECDESEMBOLSO, MTOORIGINALCREDITO, MTODESEMBOLSADO, CANAL_MDPREST. Ahora a qué registro le agregamos estos valores? Pues por ejemplo
para las solicitudes del 202301 en la fecha 2023-01.04 se ha visto que para este mismo mes se tiene una fecapertura el 2023-01-04 entonces, a este le adjudicarias. pero aca hay dos solicitudes que tienen la misma fecsolicitudevaluacion entonces para ello usaremos el CODEVALUACIONSOLICITUD donde este codigo siempre va en aumento cuando una solicitud es nueva es +1 de la ultima solicitud. Ahora usaremas esto siempre y cuando haya el mismo fecsolicitudevaluacion.
Se entiende?




