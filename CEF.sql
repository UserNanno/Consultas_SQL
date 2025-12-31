
from pyspark.sql.functions import col, trim, lit, current_timestamp, count, \
    date_format, max, substring, when, sum, concat, lpad, coalesce, \
    countDistinct, current_date, row_number
from pyspark_archetype.common.dicts import LazyDict
from pyspark_archetype.parameters.process_parameter import Joins
from pyspark_archetype.transformation.reader import Reader
from pyspark_archetype.transformation.transformer import BaseTransformer
from pyspark_archetype.transformation.writer import Writer
from .utils import Utils
from pyspark.sql import Window
from hd_resumenindicadorcomunicaciondirecta_push_module.transformation.utils import Utils_push

class Transformation(BaseTransformer, Utils, Joins):
    """
    Transformations associated with the generation of savings identification accounts (with DataFrame)
    """

    def __init__(self) -> None:
        """
        Instance from Reader and Writer
        """
        self.jobarroba = "@P1LKL3J"
        self.nombre_proceso = "CRM_COMUNICACIONMASIVA_HD_RESUMENINDICADORCO_EMAIL"
        self._reader = Reader()
        self._writer = Writer()
        self.spark = self._reader.spark

        # Se leen los parametros del json PRM dentro de la carpeta Modulo email
        self._local_parameters = self._reader.config.local_parameters

        self.current_step_email = 0
        self.total_steps_email = 0

        self._local_parameters = self._reader.config.local_parameters
        self.webhook_prm_spark = self._local_parameters["PRM_SPARK_WEBHOOK"]
        self.webhook_prm_spark_errores = self._local_parameters["PRM_SPARK_WEBHOOK_ERRORES"]
        self.ambiente_prm_spark = self._local_parameters["PRM_SPARK_ENVIRONMENT"]

        self._codapp = self._local_parameters["PRM_SPARK_COD_APP_VALUE"]
        self._codapporigenversion = self._local_parameters["PRM_SPARK_COD_APPORIGENVERSION"]
        self._fecha_rutina = self._local_parameters["PRM_SPARK_FECHA_RUTINA"]
        print("FECHA RUTINA")
        print(self._fecha_rutina)
        self._n_meses_busqueda_envio = self._local_parameters["PRM_SPARK_N_MESES_BUSQUEDA_ENVIO"]
        self._n_meses_busqueda_accion = self._local_parameters["PRM_SPARK_N_MESES_BUSQUEDA_ACCION"]

        self.prm_spark_flg_nul_dupli = self._local_parameters["PRM_SPARK_FLG_NUL_DUPLI"]
        self.CONS_DATE_FORMAT_ADOBE = "dd/MM/yyyy"

        self._keys_list = ["codapp", "codappversion", "codcanalcomunicaciondirecta",
                           "feccomunicaciondirecta", "codmescomunicaciondirecta", "numsecuencial"]
        self.fecha_rutina = self._fecha_rutina

    def _logger_info_email(self, mensaje: str) -> None:
        """
        Imprimir mensaje modo log y print
        :param mensaje: mensaje a imprimir
        :return:
        """
        self._reader.logger.warn(f":::  {mensaje} ")
        print(f":::  {mensaje} ")

    def _logger_step_e(self, mensaje: str) -> None:
        """
        Imprimir mensaje modo log y print + stepspa
        :param mensaje: mensaje a imprimir
        :return:
        """
        self.current_step_email += 1
        self.mensaje_output = f"::: [{self.current_step_email}/{self.total_steps_email}] ::: {mensaje} "
        self._reader.logger.warn(self.mensaje_output)
        print(self.mensaje_output)

    def mi_funcion_groupby_e(self, df_email, nombres_columnas):
        """
        Metodo para determinar la cantidad de valores duplicados por llave
        :param df_email: dataframe a extraer cantidad de valores duplicados por llave
        :param nombres_columnas: lista de campos llave de la tabla df, ejm: ["llave1"]
        :return: valor
        """
        return df_email.groupBy(*[col(nombre) for nombre in nombres_columnas]).count().filter(col("count") > 1).count()

    def existe_codmes_actual_prev_in_cliente(self, codmes_mes_actual_previo,
                                               h_cliente_json_key):
        # codmes_mes_actual_principalidad = codmes_mes_actual - 1 mes
        column_partition = 'fecrutina'

        if self.ambiente_prm_spark.upper() == 'LOCAL':
            partitions_df = Utils_push.get_dataframe(self, h_cliente_json_key).select(column_partition)
        else:
            name_table_json = self.get_name_table(self, h_cliente_json_key)
            partitions_df = self.spark.sql("SHOW PARTITIONS " + name_table_json)

        partition_filtrada_df = partitions_df.filter(
            date_format(col(column_partition), 'yyyyMM') == lit(codmes_mes_actual_previo)
        )

        if partition_filtrada_df.count() > 0:
            self.logger_info(f" Codmes actual previo CLIENTE {codmes_mes_actual_previo} DISPONIBLE")
            return True

        self.logger_info(f" Codmes actual previo CLIENTE {codmes_mes_actual_previo} NO DISPONIBLE")
        return False

    def filter_h_cliente_email(self, h_cliente_df, codmes_mes_anterior, codmes_mes_actual):
        return h_cliente_df.filter(
            date_format(col("fecrutina"), 'yyyyMM').between(codmes_mes_anterior, codmes_mes_actual)
        ).filter(
            col("tipclasifcli") != lit('CD')
        )

    def group_max_fecrutina_x_cliente_email(self, h_cliente_filtered_df):
        return h_cliente_filtered_df.groupBy(
            date_format(col("fecrutina"), 'yyyyMM').alias('codmes'),
            col('codclavepartycli'),
            col('tiprolcli'),
            col('codinternocomputacional')
        ).agg(max('fecrutina').alias('fecrutina_max'))

    def get_ultimo_segmento_x_cliente_y_codmes(self, h_cliente_filtered_df, h_cliente_grouped_df):

        codmes_mes_col = substring(col("CLI_G.codmes"), 5, 2)
        codmes_anio_col = substring(col("CLI_G.codmes"), 1, 4)

        return h_cliente_filtered_df.alias('CLI_F').join(
            h_cliente_grouped_df.alias('CLI_G'),
            (col("CLI_F.fecrutina") == col("CLI_G.fecrutina_max")) &
            (col("CLI_F.codinternocomputacional") == col("CLI_G.codinternocomputacional")),
            'INNER'
        ).select(
            col('CLI_G.codmes'),
            col('CLI_G.codinternocomputacional'),
            col('CLI_G.codclavepartycli'),
            col('CLI_G.tiprolcli'),
            col('CLI_F.codsegmento'),
            col('CLI_F.dessegmento'),

            when(codmes_mes_col == "12", concat(codmes_anio_col.cast("int") + 1, lit("01"))
                 ).otherwise(concat(codmes_anio_col, lpad((codmes_mes_col.cast("int") + 1).cast("string"), 2, "0"))
                             ).alias("codmes_relacionado")
        )

    def existe_codmes_actual_previo_in_principalidad_email(self, codmes_mes_actual_previo,
                                                           hm_variableclientebancapersona_json_key):
        # codmes_mes_actual_principalidad = codmes_mes_actual - 1 mes
        column_partition = 'codmes'

        if self.ambiente_prm_spark.upper() == 'LOCAL':
            partitions_df = Utils_push.get_dataframe(self, hm_variableclientebancapersona_json_key).select(column_partition)
        else:
            name_table_json = self.get_name_table(self, hm_variableclientebancapersona_json_key)
            partitions_df = self.spark.sql("SHOW PARTITIONS " + name_table_json)

        partition_filtrada_df = partitions_df.filter(
            col(column_partition) == lit(codmes_mes_actual_previo)
        )

        if partition_filtrada_df.count() > 0:
            self.logger_info(f" Codmes actual previo Principalidad {codmes_mes_actual_previo} DISPONIBLE")
            return True

        self.logger_info(f" Codmes actual previo Principalidad {codmes_mes_actual_previo} NO DISPONIBLE")
        return False

    def filter_h_variableclibanca_email(self, hm_variableclientebancapersona_df, codmes_mes_anterior_principalidad,
                                  codmes_mes_actual_principalidad):
        # codmes_mes_actual_principalidad = codmes_mes_actual - 1 mes
        codmes_mes_col = substring(col("codmes"), 5, 2)
        codmes_anio_col = substring(col("codmes"), 1, 4)

        return hm_variableclientebancapersona_df.filter(
            col("codmes").between(codmes_mes_anterior_principalidad, codmes_mes_actual_principalidad)
        ).select(
            'codmes', 'codinternocomputacional', 'codsubsegmentoconsumo',
            'dessubsegmentoconsumo', 'tipnivelprincipalidad', 'destipnivelprincipalidad',

            when(codmes_mes_col == "12", concat(codmes_anio_col.cast("int") + 1, lit("01"))
                 ).otherwise(concat(codmes_anio_col, lpad((codmes_mes_col.cast("int") + 1).cast("string"), 2, "0"))
                             ).alias("codmes_relacionado")
        )

    def add_seg_y_princ_a_envios(self, envio_df, segmento_df, principalidad_df,
                                 flag_codmes_actual_cliente, flag_codmes_actual_principalidad,
                                 codmes_mes_actual_previo, codmes_actual):

        join_cliente_codmes = self.get_condicion_join(flag_codmes_actual_cliente, "SEGMENTO",
                                                      codmes_mes_actual_previo, codmes_actual)

        join_principalidad_codmes = self.get_condicion_join(flag_codmes_actual_principalidad, "PRINCIPALIDAD",
                                                            codmes_mes_actual_previo, codmes_actual)

        return envio_df.alias("ENVIO").join(
            segmento_df.alias("SEGMENTO"),
            (join_cliente_codmes) &
            (col("ENVIO.codinternocomputacional") == col("SEGMENTO.codinternocomputacional")),
            'LEFT'
        ).join(
            principalidad_df.alias("PRINCIPALIDAD"),
            (join_principalidad_codmes) &
            (col("ENVIO.codinternocomputacional") == col("PRINCIPALIDAD.codinternocomputacional")),
            'LEFT',
        ).select(
            'ENVIO.codapp', 'ENVIO.codapporigenversion',
            'ENVIO.codenviocomunicacion', 'ENVIO.codinternocomputacional',
            'ENVIO.codclavepartycli', 'ENVIO.tiprolcli',
            'ENVIO.codcanalcomunicacion', 'ENVIO.descanalcomunicacion', 'ENVIO.codplantillaenviocomunicacion',
            'ENVIO.desdetplantillaenviocomunicacion', 'ENVIO.tipestadoenviocomunicacion',
            'ENVIO.fecenviocomunicacion', 'ENVIO.horenviocomunicacion', 'ENVIO.codmesenviocomunicacion',
            'SEGMENTO.codsegmento',
            'SEGMENTO.dessegmento',
            when(trim(col('SEGMENTO.CODSEGMENTO')) == 'PM',
                 when(col('PRINCIPALIDAD.CODSUBSEGMENTOCONSUMO').isNull(), 'C')
                 .otherwise(col('PRINCIPALIDAD.CODSUBSEGMENTOCONSUMO'))
                 ).alias('CODSUBSEGMENTOCONSUMO'),  # FIN WHEN - ALIAS
            when(trim(col('SEGMENTO.CODSEGMENTO')) == 'PM',
                 when(col('PRINCIPALIDAD.DESSUBSEGMENTOCONSUMO').isNull(), 'CARLOS')
                 .otherwise(col('PRINCIPALIDAD.DESSUBSEGMENTOCONSUMO'))
                 ).alias('DESSUBSEGMENTOCONSUMO'),  # FIN WHEN - ALIAS
            when(trim(col('SEGMENTO.CODSEGMENTO')).isin('PL', 'PX', 'PM'),
                 when(col('PRINCIPALIDAD.TIPNIVELPRINCIPALIDAD').isNull(), '6')
                 .otherwise(col('PRINCIPALIDAD.TIPNIVELPRINCIPALIDAD'))
                 ).otherwise('999').alias('TIPNIVELPRINCIPALIDAD'),  # FIN WHEN - ALIAS
            when(trim(col('SEGMENTO.CODSEGMENTO')).isin('PL', 'PX', 'PM'),
                 when(col('PRINCIPALIDAD.DESTIPNIVELPRINCIPALIDAD').isNull(), 'No Principal')
                 .otherwise(col('PRINCIPALIDAD.DESTIPNIVELPRINCIPALIDAD'))
                 ).otherwise('Sin definir').alias('DESTIPNIVELPRINCIPALIDAD'),  # FIN WHEN - ALIAS
            when(trim(col('SEGMENTO.CODSEGMENTO')).isin('PL', 'PX', 'PM'),
                 when(col('PRINCIPALIDAD.TIPNIVELPRINCIPALIDAD').isin('1'), '01ALTA')
                 .when(col('PRINCIPALIDAD.TIPNIVELPRINCIPALIDAD').isin('2', '4'), '02MEDIA')
                 .when(col('PRINCIPALIDAD.TIPNIVELPRINCIPALIDAD').isin('3', '5'), '03BAJA')
                 .otherwise('04NOPRI')
                 ).otherwise('05NODEF').alias('GLOSA_PRINCIPALIDAD')
        )

    def get_condicion_join(self, flag_codmes_actual_principalidad, tabla_relacionada,
                           codmes_mes_actual_previo, codmes_actual):
        # tabla_relacionada = alias_tabla_codmes_relacionad

        if flag_codmes_actual_principalidad == True:
            join_principalidad_codmes = (
                col("ENVIO.codmesenviocomunicacion") == col(f"{tabla_relacionada}.codmes_relacionado")
            )
        else:
            join_principalidad_codmes = when(
                col("ENVIO.codmesenviocomunicacion") == lit(codmes_actual),
                lit(codmes_mes_actual_previo) == col(f"{tabla_relacionada}.codmes_relacionado")
            ).otherwise(
                col("ENVIO.codmesenviocomunicacion") == col(f"{tabla_relacionada}.codmes_relacionado")
            )
        return join_principalidad_codmes

    def filter_h_envio_email(self, h_accionenviocomunicacion_df, codmes_mes_anterior, codmes_mes_actual):
        return h_accionenviocomunicacion_df.filter(
            col("codmesenviocomunicacion").between(codmes_mes_anterior, codmes_mes_actual)
        ).filter(
            (trim(col("tipestadoenviocomunicacion")).isin("1", "2"))  # -- 1: ENVIADO, 2: HA FALLADO
            & (trim(col("codcanalcomunicacion")).isin("0", "1", "127"))  # -- 0: EMAIL, 1: SMS , 127: WHATSAPP
            & (col("codapporigenversion") == lit(self._codapporigenversion))
            & (col("codapp") == lit(self._codapp))
        )

    def filter_h_accion_email(self, h_accionenviomensajeinstantaneo_df, codmes_mes_anterior, codmes_mes_proximo,
                              codmes_mes_actual_envio):
        # codmes_mes_proximo = codmes_mes_actual +  _n_meses_busqueda_accion

        # Se agrega el primer filtro, teniendo encuenta que solos se necessita envios con fecha entre codmes_mes_anterior y codmes_mes_actual
        # Adicionalmente sabiendo que pueden existir acciones que no esten sincronizadas , se añade el fecha envio is null
        return h_accionenviomensajeinstantaneo_df.filter(
            col("codmesaccionenviocomunicacion").between(codmes_mes_anterior, codmes_mes_proximo)
        ).filter(
            (trim(col("tipaccionenviocomunicacion")).isin("1", "2"))  # -- 2: ABIERTO, 10: CLICK EN LA NOTIF. MOVIL
            & (col("codapporigenversion") == lit(self._codapporigenversion))
            & (col("codapp") == lit(self._codapp))
        ).filter(
            date_format(col("fecenviocomunicacion"), 'yyyyMM') <= lit(codmes_mes_actual_envio)
        )

    def add_accion_a_envios_email(self, envio_df, accion_df):
        return envio_df.alias("ENVIO").join(
            accion_df.alias("ACCION"),
            (col("ACCION.CODENVIOCOMUNICACION") == col("ENVIO.CODENVIOCOMUNICACION")) &
            (col("ACCION.FECENVIOCOMUNICACION") == col("ENVIO.FECENVIOCOMUNICACION")) &
            (col("ACCION.CODAPP") == col("ENVIO.CODAPP")) &
            (col("ACCION.CODAPPORIGENVERSION") == col("ENVIO.CODAPPORIGENVERSION")),
            'LEFT'
        ).select(
            'ENVIO.codmesenviocomunicacion', 'ENVIO.codenviocomunicacion',
            'ENVIO.fecenviocomunicacion', 'ENVIO.horenviocomunicacion',
            'ENVIO.codapp', 'ENVIO.codapporigenversion',
            'ENVIO.codinternocomputacional', 'ENVIO.codclavepartycli', 'ENVIO.tiprolcli',
            'ENVIO.codcanalcomunicacion', 'ENVIO.descanalcomunicacion',
            'ENVIO.codplantillaenviocomunicacion', 'ENVIO.desdetplantillaenviocomunicacion',
            'ENVIO.codsegmento', 'ENVIO.dessegmento',
            'ENVIO.codsubsegmentoconsumo', 'ENVIO.dessubsegmentoconsumo',
            'ENVIO.tipnivelprincipalidad', 'ENVIO.destipnivelprincipalidad',
            'ENVIO.glosa_principalidad', 'ENVIO.tipestadoenviocomunicacion',
            'ACCION.tipaccionenviocomunicacion'
        )

    def group_acciones_por_envio_email(self, envios_add_accion_df):
        return envios_add_accion_df.groupBy(
            col('codapp'), col('codapporigenversion'),
            col('codenviocomunicacion'),
            col('codmesenviocomunicacion'),
            col('codinternocomputacional'), col('codclavepartycli'), col('tiprolcli'),
            col('codcanalcomunicacion'), col('descanalcomunicacion'),
            col('fecenviocomunicacion'), col('horenviocomunicacion'),
            col('codplantillaenviocomunicacion'), col('desdetplantillaenviocomunicacion'),
            col('codsegmento'), col('dessegmento'), col('codsubsegmentoconsumo'), col('dessubsegmentoconsumo'),
            col('tipnivelprincipalidad'), col('destipnivelprincipalidad'), col('glosa_principalidad'),

            when(trim(col("tipestadoenviocomunicacion")) == lit("2"), 1).otherwise(0).alias("FLG_FALLA")
            # -- 2 HA FALLADO / 1 ENVIADO
        ).agg(
            sum(  # -- tipaccionenviocomunicacion : 2: ABIERTO / 10: CLICK EN LA NOTIFICACION MOVIL
                when((trim(col("tipestadoenviocomunicacion")) == lit("1")) &
                     (trim(col("tipaccionenviocomunicacion")) == lit("2")) &
                     (trim(col('codcanalcomunicacion')) == lit(0)), 1).otherwise(0)
            ).alias("NUMAPERTURAS"),
            sum(
                when((trim(col("tipestadoenviocomunicacion")) == lit("1")) &
                     (trim(col("tipaccionenviocomunicacion")) == lit("1")) &
                     (trim(col('codcanalcomunicacion')) == lit(0)), 1).otherwise(0)
            ).alias("NUMCLICS")
        )

    def agg_medidas_comunicaciones_email(self, acciones_agrupadas_por_envio_df):
        return acciones_agrupadas_por_envio_df.groupBy(
            col('codapp'), col('codapporigenversion'),
            col('codcanalcomunicacion'), col('descanalcomunicacion'),
            col('codplantillaenviocomunicacion').alias('codplantillacomunicaciondirecta'),
            col('fecenviocomunicacion').alias("feccomunicaciondirecta"),
            substring(col('horenviocomunicacion'), 1, 2).alias("numhorcomunicaciondirecta"),
            col('codmesenviocomunicacion').alias("codmescomunicaciondirecta"),
            col('desdetplantillaenviocomunicacion').alias("desdetplantillacomunicaciondirecta"),
            col('codsegmento'), col('dessegmento'), col('codsubsegmentoconsumo'), col('dessubsegmentoconsumo'),
            col('tipnivelprincipalidad'), col('destipnivelprincipalidad'),
            col('glosa_principalidad').alias("desagrupacionnivelprincipalidad")

        ).agg(
            count(lit(1)).alias("ctdcomunicaciondirecta"),
            sum(col("FLG_FALLA")).alias("ctdfallacomunicaciondirecta"),
            sum(col("NUMAPERTURAS")).alias("ctdaperturatotalcomunicaciondirecta"),
            countDistinct(when(col("NUMAPERTURAS") > 0, col("codenviocomunicacion")).otherwise(None)
                          ).alias("ctdaperturaunicacomunicaciondirecta"),
            sum(col("NUMCLICS")).alias("ctdclictotalcomunicaciondirecta"),
            countDistinct(when(col("NUMCLICS") > 0, col("codenviocomunicacion")).otherwise(None)).alias(
                "ctdcliclunicacomunicaciondirecta")
        )

    def formateo_tabla_email_final(self, pre_indicadores_mail_df):
        window_spec = Window.partitionBy("codapp", "codapporigenversion", "codcanalcomunicacion",
                                         "codmescomunicaciondirecta") \
            .orderBy("codplantillacomunicaciondirecta", "feccomunicaciondirecta", "numhorcomunicaciondirecta",
                     "codsegmento", "dessegmento", "descanalcomunicacion", "desdetplantillacomunicaciondirecta",
                     "ctdcomunicaciondirecta", "ctdaperturatotalcomunicaciondirecta", "ctdaperturaunicacomunicaciondirecta",
                     "ctdclictotalcomunicaciondirecta", "ctdcliclunicacomunicaciondirecta", "ctdfallacomunicaciondirecta",
                     "tipnivelprincipalidad", "destipnivelprincipalidad", "desagrupacionnivelprincipalidad",
                     "codsubsegmentoconsumo", "dessubsegmentoconsumo")
        indicadores_mail_sec_df = pre_indicadores_mail_df.select(*pre_indicadores_mail_df.columns,
                                                           row_number().over(window_spec).alias("numsecuencial"))
        return indicadores_mail_sec_df.select(
            col('codmescomunicaciondirecta').alias("codmescomunicaciondirecta"),
            col('codcanalcomunicacion').alias("codcanalcomunicaciondirecta"),
            col('codapp'),
            col('codapporigenversion').alias("codappversion"),
            col("numsecuencial"),
            col('feccomunicaciondirecta').alias("feccomunicaciondirecta"),
            col('codplantillacomunicaciondirecta').alias("codplantillacomunicaciondirecta"),
            col("numhorcomunicaciondirecta"),
            col('codsegmento'),
            col('dessegmento'),
            col('descanalcomunicacion').alias("descanalcomunicaciondirecta"),
            col('desdetplantillacomunicaciondirecta').alias("desdetplantillacomunicaciondirecta"),
            date_format(col('feccomunicaciondirecta'), 'd').alias("numdiamescomunicaciondirecta"),
            self.get_numero_semana_comunicacion_email(col('feccomunicaciondirecta')
                                                      ).alias("numsemanamescomunicaciondirecta"),
            self.get_dia_semana_comunicacion_email(col('feccomunicaciondirecta')
                                                   ).alias("numdiasemanacomunicaciondirecta"),
            col("ctdcomunicaciondirecta"),
            col('ctdaperturatotalcomunicaciondirecta'),
            col('ctdaperturaunicacomunicaciondirecta'),
            lit(0).alias("ctdimpresiontotalcomunicaciondirecta"),
            lit(0).alias("ctdimpresionunicacomunicaciondirecta"),
            col('ctdclictotalcomunicaciondirecta'),
            col('ctdcliclunicacomunicaciondirecta').alias("ctdclicunicacomunicaciondirecta"),
            lit(0).alias("ctdclicnotificaciontotalcomunicaciondirecta"),
            lit(0).alias("ctdclicnotificacionunicacomunicaciondirecta"),
            col("ctdfallacomunicaciondirecta"),
            lit(0).alias("flgcomunicaciondirectaenlace"),
            col('tipnivelprincipalidad'),
            col('destipnivelprincipalidad'),
            col('desagrupacionnivelprincipalidad').alias("desagrupacionnivelprincipalidad"),
            col('codsubsegmentoconsumo'),
            col('dessubsegmentoconsumo'),
            lit(self._fecha_rutina).alias("fecrutina"),
            lit(current_date()).alias("fecactualizacionregistro"),
            lit("D").alias("tipfrecuenciaregistro")
        )

    @staticmethod
    def get_numero_semana_comunicacion_email(colums_fecha_envio_email):
        return when(date_format(colums_fecha_envio_email, 'd').between(1, 7), lit(1)
                    ).when(date_format(colums_fecha_envio_email, 'd').between(8, 14), lit(2)
                           ).when(date_format(colums_fecha_envio_email, 'd').between(15, 21), lit(3)
                                  ).otherwise(lit(4))

    @staticmethod
    def get_dia_semana_comunicacion_email(colums_fecha_envio_email):
        return when(
            date_format(colums_fecha_envio_email, 'E') == lit("Mon"), lit(1)
        ).when(
            date_format(colums_fecha_envio_email, 'E') == lit("Tue"), lit(2)
        ).when(
            date_format(colums_fecha_envio_email, 'E') == lit("Wed"), lit(3)
        ).when(
            date_format(colums_fecha_envio_email, 'E') == lit("Thu"), lit(4)
        ).when(
            date_format(colums_fecha_envio_email, 'E') == lit("Fri"), lit(5)
        ).when(
            date_format(colums_fecha_envio_email, 'E') == lit("Sat"), lit(6)
        ).when(
            date_format(colums_fecha_envio_email, 'E') == lit("Sun"), lit(7)
        )

    @Utils.AlertaTeams
    def transformer(self) -> None:
        """
        Entry point for building the savings identification account database
        """
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

        self._logger_info_email("==========================  INICIO PROCESO ==================================")
        # Parametros
        self.logger_info(f"> _n_meses_busqueda_envio : {self._n_meses_busqueda_envio} ")
        self.logger_info(f"> _n_meses_busqueda_accion : {self._n_meses_busqueda_accion} ")
        self.logger_info(f"> _fecha_rutina : {self._fecha_rutina} ")
        self.logger_info(f"> _codapp : {self._codapp} ")
        self.logger_info(f"> _codapporigenversion : {self._codapporigenversion} ")

        # Lectura de inputs
        hm_variableclientebancapersona_json_key = 'hm_variableclientebancapersona'
        h_enviocomunicacion_df = Utils_push.get_dataframe(self, "h_envio")
        h_accionenviocomunicacion_df = Utils_push.get_dataframe(self, "h_accionenvio")
        hm_variableclientebancapersona_df = Utils_push.get_dataframe(self, hm_variableclientebancapersona_json_key)
        h_cliente_df = Utils_push.get_dataframe(self, "h_cliente")

        # Calculo de fechas limites para accion y envio  sumar_n_mes_fecha_anio_mes_dia_email
        fecha_rutina_mes_anterior = Utils_push.restar_n_mes_fecha_anio_mes_dia(self._fecha_rutina,
                                                                               self._n_meses_busqueda_envio)
        fecha_rutina_mes_proximo = Utils_push.sumar_n_mes_fecha_anio_mes_dia(self._fecha_rutina,
                                                                             self._n_meses_busqueda_accion)
        codmes_mes_actual = Utils_push.convert_fecha_anio_mes_dia_a_codigo_anio_mes(self._fecha_rutina)
        codmes_mes_anterior = Utils_push.convert_fecha_anio_mes_dia_a_codigo_anio_mes(fecha_rutina_mes_anterior)
        codmes_mes_proximo = Utils_push.convert_fecha_anio_mes_dia_a_codigo_anio_mes(fecha_rutina_mes_proximo)

        # Calculo de fechas limites para principalidad y segmento se trabaja con codmes -1
        fecha_rutina_mes_actual_1mes_previo = Utils_push.restar_n_mes_fecha_anio_mes_dia(self._fecha_rutina, 1)
        fecha_rutina_mes_anterior_1mes_previo = Utils_push.restar_n_mes_fecha_anio_mes_dia(fecha_rutina_mes_anterior, 1)

        codmes_mes_actual_1mes_previo = Utils_push.convert_fecha_anio_mes_dia_a_codigo_anio_mes(
            fecha_rutina_mes_actual_1mes_previo)
        codmes_mes_anterior_1mes_previo = Utils_push.convert_fecha_anio_mes_dia_a_codigo_anio_mes(
            fecha_rutina_mes_anterior_1mes_previo)

        self.logger_info(f"> codmes_mes_anterior : {codmes_mes_anterior} ")
        self.logger_info(f"> codmes_mes_actual : {codmes_mes_actual} ")
        self.logger_info(f"> codmes_mes_proximo (Para acciones) : {codmes_mes_proximo} ")

        self.logger_info(f"> codmes_mes_anterior_1mes_previo : {codmes_mes_anterior_1mes_previo} ")
        self.logger_info(f"> codmes_mes_actual_1mes_previo : {codmes_mes_actual_1mes_previo} ")

        # Transformaciones
        # OBTENER DE LA TABLA DE CLIENTES EL SEGMENTO MAS RECIENTE PARA CADA MES (ANTERIOR Y ACTUAL)
        h_cliente_json_key = 'h_cliente'
        flag_codmes_actual_cliente = self.existe_codmes_actual_prev_in_cliente(codmes_mes_actual_1mes_previo,
                                                                               h_cliente_json_key)

        cliente_filtered_codmes_df = self.filter_h_cliente_email(h_cliente_df, codmes_mes_anterior_1mes_previo,
                                                                 codmes_mes_actual_1mes_previo)

        cliente_max_fecrutina_x_cliente_df = self.group_max_fecrutina_x_cliente_email(cliente_filtered_codmes_df)

        cliente_segmento_por_codmes = self.get_ultimo_segmento_x_cliente_y_codmes(cliente_filtered_codmes_df,
                                                                                    cliente_max_fecrutina_x_cliente_df)

        # PASO 1: EXTRAEMOS TODOS LOS ENVIOS DE DOS MESES: MES ACTUAL Y MES ANTERIOR CERRADO
        envio_filtered_codmes_df = self.filter_h_envio_email(h_enviocomunicacion_df,
                                                             codmes_mes_anterior, codmes_mes_actual)

        # PASO 2: PINTAMOS LOS DATOS DE SEGMENTO Y PRINCIPALIDAD A LA TABLA DE ENVIOS
        flag_codmes_actual_principalidad = self.existe_codmes_actual_previo_in_principalidad_email(
            codmes_mes_actual_1mes_previo,
            hm_variableclientebancapersona_json_key)

        principalidad_filtered_codmes_df = self.filter_h_variableclibanca_email(hm_variableclientebancapersona_df,
                                                                                codmes_mes_anterior_1mes_previo,
                                                                                codmes_mes_actual_1mes_previo)

        envios_add_seg_prin_df = self.add_seg_y_princ_a_envios(envio_filtered_codmes_df,
                                                               cliente_segmento_por_codmes,
                                                               principalidad_filtered_codmes_df,
                                                               flag_codmes_actual_cliente,
                                                               flag_codmes_actual_principalidad,
                                                               codmes_mes_actual_1mes_previo,
                                                               codmes_mes_actual
                                                               )

        # PASO 3: EXTRAEMOS TODAS LAS ACCIONES LIGADAS A LOS CODENVIOMENSAJEINSTANTANEO OBTENIDAS EN EL PASO ANTERIOR
        accion_filtered_codmes_df = self.filter_h_accion_email(h_accionenviocomunicacion_df,
                                                               codmes_mes_anterior, codmes_mes_proximo, codmes_mes_actual)

        envios_add_accion_df = self.add_accion_a_envios_email(envios_add_seg_prin_df, accion_filtered_codmes_df)

        # PASO 4: UNIFICAMOS TABLAS DE ENVIOS CON LA DE ACCIONES (AGRUPAMOS)
        acciones_agrupadas_por_envio_df = self.group_acciones_por_envio_email(envios_add_accion_df)

        # PASO 5: OBTENER LA TABLA DE CONTACTABILIDAD DE COMUNICACIONES PARA PUSH
        agg_medidas_email_df = self.agg_medidas_comunicaciones_email(acciones_agrupadas_por_envio_df)

        # FORMATEO FINAL
        indicadores_formated_df = self.formateo_tabla_email_final(agg_medidas_email_df)

        indicadores_formated_df.persist()
        print("DF formateo final")
        indicadores_formated_df.show(5)
        resumen_indicadores_df, resumen_indicadores_rej_df = Utils_push.separar_unicos_de_rejectados(indicadores_formated_df,
                                                                                                     self._keys_list)

        # IMPRESIÓN DE CANTIDAD DE REGISTROS FINALES
        cantidad_registros = resumen_indicadores_df.count()
        self.logger_info(f">>> CANTIDAD DE REGISTROS DF FINAL :: {cantidad_registros} ")
        cantidad_rej_registros = resumen_indicadores_rej_df.count()
        self.logger_info(f">>> CANTIDAD DE REGISTROS REJECTADOS DF FINAL :: {cantidad_rej_registros} ")

        #### ESCRITURA EN RUTAS FINALES
        dfs = LazyDict({
            "HD_RESUMENINDICADORCOMUNICACIONDIRECTA": resumen_indicadores_df,
            "HD_RESUMENINDICADORCOMUNICACIONDIRECTA_REJ": resumen_indicadores_rej_df
        })
        self._writer.write(dfs)

        indicadores_formated_df.unpersist()

        # Parametros ALERTA TEAMS
        detalle_alerta_teams = {}
        detalle_alerta_teams['HD_RESUMENINDICADORCOMUNICACIONDIRECTA'] = str(cantidad_registros)
        detalle_alerta_teams['HD_RESUMENINDICADORCOMUNICACIONDIRECTA_REJ'] = str(cantidad_rej_registros)

        return detalle_alerta_teams
