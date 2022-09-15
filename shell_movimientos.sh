#!/bin/bash
##########################################################################
# Script de carga de datos de tabla db_desarrollo2021.otc_t_360_general  #
#  para cruzarlos con catalogos y generar la tabla:                      #
# db_desarrollo2021.otc_t_movimientos                                    #
# Creado 2022/22/06  (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#
#																		 #
# Dearrollado por: Cristian Ortiz (NAE108834) SOFTCONSULTING             #
# Fecha de modificacion:   2022/23/06                                    #
#																		 #
#------------------------------------------------------------------------#
#																		 #
# Modificado por: XXXXXXXXXXXXX (XXXXXXXXX) SOFTCONSULTING               #
# Fecha de modificacion:   2022/05/16                                    #
##########################################################################


#PARAMETROS ESTATICOS
ENTIDAD=D_MOVIMIENTOS1

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_RUTA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_RUTA';"`
VAL_NOMBRE_ARCHIVO=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_NOMBRE_ARCHIVO';"`

VAL_FTP_PUERTO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_PUERTO';"`
VAL_FTP_USER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_USER';"`
VAL_FTP_HOSTNAME=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_HOSTNAME';"`
VAL_FTP_PASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_PASS';"`
VAL_FTP_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND PARAMETRO = 'VAL_FTP_RUTA';"`


#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_FECHA_MESES=$(date -d "$VAL_FECHA_EJEC -6 month" +%Y%m%d)
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'` 
VAL_LOG=$VAL_RUTA/log/OTC_T_DSCNTS_ACTIVOS_INACTIVOS_$VAL_DIA$VAL_HORA.log


version=1.2.1000.2.6.4.0-91
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

HIVE_HOME=/usr/hdp/current/hive-client
HCAT_HOME=/usr/hdp/current/hive-webhcat

##SHELL KARI CASTRO

#PARAMETROS GENERICOS
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_USER';"`

#Modificas esta comprobacion
if [ -z "$VAL_RUTA" ] || [ -z "$VAL_COLA_EJECUCION" ] || [ -z "$VAL_CADENA_JDBC" ] ||  [ -z "$VAL_USER" ] || [ -z "$VAL_NOMBRE_ARCHIVO" ] || [ -z "$VAL_FECHAEJE" ] ; then
	echo " ERROR - uno de los parametros esta vacio o nulo"
	exit 1
fi

echo "==== Ejecuta el HQL que genera el Extractor de Movimientos ====" >> $VAL_LOG
beeline -u $VAL_CADENA_JDBC -n $VAL_USUARIO --hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
--hiveconf hive.query.name=OTC_T_HISPAM_POSPAGO --outputformat=dsv --delimiterForDSV='|' --showHeader=true -e "set hive.cli.print.header=true;
SELECT fecha_activacion,fecha_proceso,anio,mes,es_parque,segmento_bi,sub_segmento_bi,codigo_cliente,identificador_cliente,num_abonado,estado_abonado,telefono,codigo_plan,desplan,categoria_plan,tarifa_basica,forma_pago_alta,forma_pago_factura,tarjeta_banco,ciclo_factura,tac,fecha_alta_historia,canal_alta,sub_canal_alta,distribuidor_alta,oficina_alta,portabilidad,operadora_origen,operadora_destino,motivo,fecha_pre_pos,canal_pre_pos,sub_canal_pre_pos,distribuidor_pre_pos,oficina_pre_pos,fecha_pos_pre,canal_pos_pre,sub_canal_pos_pre,distribuidor_pos_pre,oficina_pos_pre,fecha_cambio_plan,canal_cambio_plan,sub_canal_cambio_plan,distribuidor_cambio_plan,oficina_cambio_plan,cod_plan_anterior,des_plan_anterior,tb_descuento,tb_override,delta,canal_movimiento_mes,sub_canal_movimiento_mes,distribuidor_movimiento_mes,oficina_movimiento_mes,portabilidad_movimiento_mes,operadora_origen_movimiento_mes,operadora_destino_movimiento_mes,motivo_movimiento_mes,cod_plan_anterior_movimiento_mes,des_plan_anterior_movimiento_mes,tb_descuento_movimiento_mes,tb_override_movimiento_mes,delta_movimiento_mes,origen_alta_segmento,fecha_alta_segmento,tipo_movimiento_mes,ingreso_plan_ciclo,otros_ingresos_ciclo,descuento_plan_ciclo,otros_descuentos_ciclo,ingreso_equipo_no_ciclo,otros_ingresos_no_ciclo,descuento_equipo_no_ciclo,ingresos,ingreso_ciclo,factura_ciclo,descuento_ciclo,ingreso_no_ciclo,factura_no_ciclo,descuento_no_ciclo,nota_credito,parque_recargadador,valor_recargas_mes0,cant_recargas_mes0,valor_recargas_mes1,cant_recargas_mes1,valor_recargas_mes2,cant_recargas_mes2,valor_recargas_mes3,cant_recargas_mes3,valor_recarga_prom_3m,cantidad_recarga_prom_3m,tecnologia,total_2g,total_3g,total_4g,mb_dentro_paquete,mb_adicional,mb_ondemand,mb_totales_cobrados,valor_mb_adicional_no_tasado,valor_mb_ondemand_no_tasado,valor_mb_total_no_tasado,valor_mb_adicional,valor_mb_ondemand,cantidad_minutos_onnet,cantidad_minutos_free,cantidad_minutos_offnetfijo,cantidad_minutos_offnetmovil,cantidad_minutos_offnet,cantidad_minutos_ldi,valor_minutos_onnet,valor_minutos_offnetfijo,valor_minutos_offnetmovil,valor_minutos_offnet,valor_minutos_ldi,valor_minutos,cantidad_llamadas,cantidad_minutos,valor_sms_dp_offnet,valor_sms_dp_onnet,valor_sms_dp_ro,valor_sms_fp_otros,valor_sms_fp_ro,valor_sms_dp_otros,valor_sms_fp_offnet,cantidad_sms_dp_offnet,cantidad_sms_dp_onnet,cantidad_sms_dp_ro,cantidad_sms_fp_otros,cantidad_sms_fp_ro,cantidad_sms_dp_otros,cantidad_sms_fp_offnet,valor_minutos_dp_offnet,valor_minutos_dp_onnet,valor_minutos_dp_ro,valor_minutos_fp_otros,valor_minutos_fp_ro,valor_minutos_dp_otros,valor_minutos_fp_offnet,cantidad_minutos_dp_offnet,cantidad_minutos_dp_onnet,cantidad_minutos_dp_ro,cantidad_minutos_fp_otros,cantidad_minutos_fp_ro,cantidad_minutos_dp_otros,cantidad_minutos_fp_offnet,provincia,canton,parroquia,celda,account_num FROM db_reportes.otc_t_hispam_pospago WHERE pt_mes=${VAL_PT_FECHA};" 2>> $VAL_LOG | sed 's/NULL//g' > ${VAL_RUTA}/output/$VAL_NOM_ARCHIVO$VAL_PT_FECHA.csv

