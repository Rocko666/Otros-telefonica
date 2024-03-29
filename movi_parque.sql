--HQL REFACTORING 360_MOVIMIENTOS_PARQUE


set hive.cli.print.header=false;	
	set hive.vectorized.execution.enabled=false;
	set hive.vectorized.execution.reduce.enabled=false;
	set tez.queue.name=$VAL_COLA_EJECUCION; --SE CAMBIO EN RF

--ELIMINA LA DATA PRE EXISTENTE
DELETE FROM ${ESQUEMA_TABLA}.OTC_T_ALTA_BAJA_HIST WHERE TIPO='ALTA' AND FECHA BETWEEN '${f_inicio}' AND '${fecha_proceso}'  ;
	
--INSERTA LA DATA DEL MES
INSERT INTO ${ESQUEMA_TABLA}.OTC_T_ALTA_BAJA_HIST
SELECT 'ALTA' AS TIPO , TELEFONO,
FECHA_ALTA AS FECHA, 
CANAL_COMERCIAL AS CANAL,
SUB_CANAL, 
CAST( NULL AS STRING) as NUEVO_SUB_CANAL,
PORTABILIDAD, 
Operadora_origen,
'MOVISTAR (OTECEL)' as Operadora_destino,
CAST( NULL AS STRING) as motivo,		   
NOM_DISTRIBUIDOR as DISTRIBUIDOR,
OFICINA,
--INSERTADO EN RF
IMEI, EQUIPO, ICC,
COD_CATEGORIA,
DOMAIN_LOGIN_OW, NOMBRE_USUARIO_OW,
DOMAIN_LOGIN_SUB, NOMBRE_USUARIO_SUB,
COD_DA, NOM_USUARIO,
CAMPANIA, CODIGO_DISTRIBUIDOR,
CODIGO_PLAZA, NOM_PLAZA, REGION, 
PROVINCIA_IVR,
EJECUTIVO_ASIGNADO_PTR, AREA_PTR, 
CODIGO_VENDEDOR_DA_PTR, JEFATURA_PTR,
PROVINCIA_MS,
CODIGO_USUARIO,
CALF_RIESGO, CAP_ENDEU, VALOR_CRED
	  
FROM db_cs_altas.otc_t_altas_bi WHERE p_FECHA_PROCESO='${fecha_movimientos_cp}' and marca ='TELEFONICA';

--ELIMINA LA DATA PRE EXISTENTE
DELETE FROM ${ESQUEMA_TABLA}.OTC_T_ALTA_BAJA_HIST WHERE TIPO='BAJA' AND FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}';

--INSERTA LA DATA DEL MES
INSERT INTO ${ESQUEMA_TABLA}.OTC_T_ALTA_BAJA_HIST
SELECT 'BAJA' AS TIPO ,TELEFONO,
FECHA_BAJA AS FECHA, 
CAST( NULL AS STRING) AS CANAL,
CAST( NULL AS STRING) AS    SUB_CANAL, 
CAST( NULL AS STRING) as NUEVO_SUB_CANAL,
PORTABILIDAD, 
'MOVISTAR (OTECEL)' AS  Operadora_origen,
Operadora_destino,
MOTIVO_BAJA as motivo,
CAST( NULL AS STRING) as DISTRIBUIDOR , 
CAST( NULL AS STRING) AS OFICINA,
--INSERTADO EN RF
EJECUTIVO_ASIGNADO_PTR, AREA_PTR, 
CODIGO_VENDEDOR_DA_PTR, JEFATURA_PTR

FROM db_cs_altas.otc_t_BAJAS_bi WHERE p_FECHA_PROCESO='${fecha_movimientos_cp}' and marca ='TELEFONICA';

--ELIMINA LA DATA PRE EXISTENTE DEL MES QUE SE PROCESA
DELETE FROM ${ESQUEMA_TABLA}.OTC_T_TRANSFER_HIST WHERE TIPO='PRE_POS' AND FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}'  ;

--INSERTA LA DATA DEL MES
INSERT INTO ${ESQUEMA_TABLA}.OTC_T_TRANSFER_HIST
SELECT 'PRE_POS' AS TIPO,
TELEFONO,
FECHA_TRANSFERENCIA AS FECHA, 
CANAL_USUARIO as CANAL,
SUB_CANAL, 
CAST( NULL AS STRING) AS NUEVO_SUB_CANAL,         
NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR, 
OFICINA_USUARIO AS OFICINA,
--INSERTADO EN RF
IMEI, EQUIPO, ICC,
DOMAIN_LOGIN_OW, NOMBRE_USUARIO_OW,
DOMAIN_LOGIN_SUB, NOMBRE_USUARIO_SUB,
CAMPANIA, CODIGO_DISTRIBUIDOR,
CODIGO_PLAZA, NOM_PLAZA, REGION,
EJECUTIVO_ASIGNADO_PTR, AREA_PTR, 
CODIGO_VENDEDOR_DA_PTR, JEFATURA_PTR,
CODIGO_USUARIO,
CALF_RIESGO, CAP_ENDEU, VALOR_CRED,
ACCOUNT_NUM_ANTERIOR, CIUDAD_USUARIO, 
PROVINCIA_USUARIO, MISMO_CLIENTE 


FROM db_cs_altas.otc_t_transfer_in_bi WHERE p_FECHA_PROCESO='${fecha_movimientos_cp}';

--ELIMINA LA DATA PRE EXISTENTE
DELETE FROM ${ESQUEMA_TABLA}.OTC_T_TRANSFER_HIST WHERE TIPO='POS_PRE' AND FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}'  ;

--INSERTA LA DATA DEL MES
INSERT INTO ${ESQUEMA_TABLA}.OTC_T_TRANSFER_HIST
SELECT 'POS_PRE' AS TIPO,
TELEFONO,
FECHA_TRANSFERENCIA AS FECHA, 
CANAL_USUARIO as CANAL,
SUB_CANAL, 
CAST( NULL AS STRING) AS NUEVO_SUB_CANAL,         
NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR, 
OFICINA_USUARIO AS OFICINA,
--INSERTADO EN RF
IMEI, EQUIPO, ICC,
DOMAIN_LOGIN_OW, NOMBRE_USUARIO_OW,
DOMAIN_LOGIN_SUB, NOMBRE_USUARIO_SUB,
CAMPANIA, CODIGO_DISTRIBUIDOR,
CODIGO_PLAZA, NOM_PLAZA, REGION,
EJECUTIVO_ASIGNADO_PTR, AREA_PTR, 
CODIGO_VENDEDOR_DA_PTR, JEFATURA_PTR,
CODIGO_USUARIO,
ACCOUNT_NUM_ANTERIOR, CIUDAD_USUARIO, 
PROVINCIA_USUARIO, MISMO_CLIENTE 

FROM db_cs_altas.otc_t_transfer_OUT_bi WHERE p_FECHA_PROCESO='${fecha_movimientos_cp}';

--ELIMINA LA DATA PRE EXISTENTE	
DELETE FROM ${ESQUEMA_TABLA}.OTC_T_CAMBIO_PLAN_HIST WHERE FECHA BETWEEN '${f_inicio}' AND '${fecha_proceso}';

--INSERTA LA DATA DEL MES
INSERT INTO ${ESQUEMA_TABLA}.OTC_T_CAMBIO_PLAN_HIST
SELECT TIPO_MOVIMIENTO AS TIPO,
TELEFONO,
FECHA_CAMBIO_PLAN AS FECHA,
CANAL,
SUB_CANAL,
CAST( NULL AS STRING) AS NUEVO_SUB_CANAL, 
NOM_DISTRIBUIDOR AS DISTRIBUIDOR ,
OFICINA, 
CODIGO_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR, 
DESCRIPCION_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR, 
TARIFA_OV_PLAN_ANT AS  TB_DESCUENTO, 
DESCUENTO_TARIFA_PLAN_ANT AS TB_OVERRIDE, 
DELTA AS DELTA,
--INSERTADO EN RF
DOMAIN_LOGIN_OW, NOMBRE_USUARIO_OW,
DOMAIN_LOGIN_SUB, NOMBRE_USUARIO_SUB,
CAMPANIA, CODIGO_DISTRIBUIDOR,
CODIGO_PLAZA, NOM_PLAZA, REGION,
NOMBRE_PLAN_ANTERIOR, TARIFA_BASICA_ANTERIOR,
FECHA_INICIO_PLAN_ANTERIOR,
TARIFA_FINAL_PLAN_ACT, TARIFA_FINAL_PLAN_ANT

FROM db_cs_altas.otc_t_cambio_plan_bi WHERE p_FECHA_PROCESO=${fecha_movimientos_cp};

--OBTIENE EL یTIMO EVENTO DEL ALTA EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE ${ESQUEMA_TABLA}.OTC_T_ALTA_HIST_UNIC;
CREATE TABLE ${ESQUEMA_TABLA}.OTC_T_ALTA_HIST_UNIC AS
SELECT
XX.TIPO , 
XX.TELEFONO,
XX.FECHA, 
XX.CANAL,
XX.SUB_CANAL, 
XX.NUEVO_SUB_CANAL,
XX.PORTABILIDAD, 
XX.Operadora_origen,
XX.Operadora_destino,
XX.motivo,		   
XX.DISTRIBUIDOR , 
XX.OFICINA
from
(
SELECT
AA.TIPO , 
AA.TELEFONO,
AA.FECHA, 
AA.CANAL,
AA.SUB_CANAL, 
AA.NUEVO_SUB_CANAL,
AA.PORTABILIDAD, 
AA.Operadora_origen,
AA.Operadora_destino,
AA.motivo,		   
AA.DISTRIBUIDOR , 
AA.OFICINA
, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
FROM ${ESQUEMA_TABLA}.OTC_T_ALTA_BAJA_HIST AS AA
WHERE FECHA <'${fecha_movimientos}'
AND TIPO='ALTA'
) XX
where XX.rnum = 1
;

--OBTIENE EL یTIMO EVENTO DE LAS BAJAS EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE ${ESQUEMA_TABLA}.OTC_T_BAJA_HIST_UNIC;
CREATE TABLE ${ESQUEMA_TABLA}.OTC_T_BAJA_HIST_UNIC AS
SELECT
XX.TIPO , 
XX.TELEFONO,
XX.FECHA, 
XX.CANAL,
XX.SUB_CANAL, 
XX.NUEVO_SUB_CANAL,
XX.PORTABILIDAD, 
XX.Operadora_origen,
XX.Operadora_destino,
XX.motivo,		   
XX.DISTRIBUIDOR , 
XX.OFICINA
from
(
SELECT
AA.TIPO , 
AA.TELEFONO,
AA.FECHA, 
AA.CANAL,
AA.SUB_CANAL, 
AA.NUEVO_SUB_CANAL,
AA.PORTABILIDAD, 
AA.Operadora_origen,
AA.Operadora_destino,
AA.motivo,		   
AA.DISTRIBUIDOR , 
AA.OFICINA
, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
FROM ${ESQUEMA_TABLA}.OTC_T_ALTA_BAJA_HIST AS AA
WHERE FECHA <'${fecha_movimientos}'
AND TIPO='BAJA'
) XX
where XX.rnum = 1
;

--OBTIENE EL یTIMO EVENTO DE LAS TRANSFERENCIAS OUT EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE ${ESQUEMA_TABLA}.OTC_T_POS_PRE_HIST_UNIC;
CREATE TABLE ${ESQUEMA_TABLA}.OTC_T_POS_PRE_HIST_UNIC AS
select
XX.TIPO , 
XX.TELEFONO,
XX.FECHA, 
XX.CANAL,
XX.SUB_CANAL, 
XX.NUEVO_SUB_CANAL,
XX.DISTRIBUIDOR , 
XX.OFICINA
from
(
SELECT
AA.TIPO , 
AA.TELEFONO,
AA.FECHA, 
AA.CANAL,
AA.SUB_CANAL, 
AA.NUEVO_SUB_CANAL,
AA.DISTRIBUIDOR , 
AA.OFICINA
, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
FROM ${ESQUEMA_TABLA}.OTC_T_TRANSFER_HIST AS AA
WHERE FECHA <'${fecha_movimientos}'
AND TIPO='POS_PRE'
) XX
where XX.rnum = 1
;

--OBTIENE EL یTIMO EVENTO DE LAS TRANSFERENCIAS IN  EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE ${ESQUEMA_TABLA}.OTC_T_PRE_POS_HIST_UNIC;
CREATE TABLE ${ESQUEMA_TABLA}.OTC_T_PRE_POS_HIST_UNIC AS
select
XX.TIPO , 
XX.TELEFONO,
XX.FECHA, 
XX.CANAL,
XX.SUB_CANAL, 
XX.NUEVO_SUB_CANAL,
XX.DISTRIBUIDOR , 
XX.OFICINA
from
(
SELECT
AA.TIPO , 
AA.TELEFONO,
AA.FECHA, 
AA.CANAL,
AA.SUB_CANAL, 
AA.NUEVO_SUB_CANAL,
AA.DISTRIBUIDOR , 
AA.OFICINA
, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
FROM ${ESQUEMA_TABLA}.OTC_T_TRANSFER_HIST AS AA
WHERE FECHA <'${fecha_movimientos}'
AND TIPO='PRE_POS'
) XX
where XX.rnum = 1
;

--OBTIENE EL یTIMO EVENTO DE LOS CAMBIOS DE PLAN EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE ${ESQUEMA_TABLA}.OTC_T_CAMBIO_PLAN_HIST_UNIC;
CREATE TABLE ${ESQUEMA_TABLA}.OTC_T_CAMBIO_PLAN_HIST_UNIC AS
SELECT
XX.TIPO , 
XX.TELEFONO,
XX.FECHA, 
XX.CANAL,
XX.SUB_CANAL, 
XX.NUEVO_SUB_CANAL,
XX.DISTRIBUIDOR , 
XX.OFICINA,
XX.COD_PLAN_ANTERIOR, 
XX.DES_PLAN_ANTERIOR, 
XX.TB_DESCUENTO, 
XX.TB_OVERRIDE, 
XX.DELTA
from
(
SELECT
AA.TIPO , 
AA.TELEFONO,
AA.FECHA, 
AA.CANAL,
AA.SUB_CANAL, 
AA.NUEVO_SUB_CANAL,
AA.DISTRIBUIDOR , 
AA.OFICINA,
AA.COD_PLAN_ANTERIOR, 
AA.DES_PLAN_ANTERIOR, 
AA.TB_DESCUENTO, 
AA.TB_OVERRIDE, 
AA.DELTA
, ROW_NUMBER() OVER (PARTITION BY aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
FROM ${ESQUEMA_TABLA}.OTC_T_CAMBIO_PLAN_HIST AS AA
WHERE FECHA <'${fecha_movimientos}'
) XX
where XX.rnum = 1;


--REALIZAMOS EL CRUCE CON CADA TABLA USANDO LA TABLA PIVOT (TABLA RESULTANTE DE PIVOT_PARQUE) 
--Y AGREANDO LOS CAMPOS DE CADA TABLA RENOMBRANDOLOS DE ACUERDO AL MOVIEMIENTO QUE CORRESPONDA.
--ESTA ES LA PRIMERA TABLA RESULTANTE QUE SERVIRA PARA ALIMENTAR LA ESTRUCTURA OTC_T_360_GENERAL.

DROP TABLE ${ESQUEMA_TEMP}.otc_t_360_parque_1_tmp_t_mov;
CREATE TABLE ${ESQUEMA_TEMP}.otc_t_360_parque_1_tmp_t_mov AS 
SELECT
NUM_TELEFONICO,
CODIGO_PLAN,
FECHA_ALTA,
FECHA_LAST_STATUS,
ESTADO_ABONADO,
FECHA_PROCESO,
NUMERO_ABONADO,
LINEA_NEGOCIO,
ACCOUNT_NUM,
SUB_SEGMENTO,
TIPO_DOC_CLIENTE,
IDENTIFICACION_CLIENTE,
CLIENTE,
CUSTOMER_REF,
COUNTED_DAYS,
LINEA_NEGOCIO_HOMOLOGADO,
CATEGORIA_PLAN,
TARIFA,
NOMBRE_PLAN,
MARCA,
CICLO_FACT,
CORREO_CLIENTE_PR,
TELEFONO_CLIENTE_PR,
IMEI,
ORDEN,
TIPO_MOVIMIENTO_MES,
FECHA_MOVIMIENTO_MES,
ES_PARQUE,
BANCO,
A.FECHA AS FECHA_ALTA_HISTORICA,
A.CANAL AS CANAL_ALTA,
A.SUB_CANAL AS SUB_CANAL_ALTA,
A.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA,
A.DISTRIBUIDOR AS DISTRIBUIDOR_ALTA,
A.OFICINA AS OFICINA_ALTA,
PORTABILIDAD,
OPERADORA_ORIGEN,
OPERADORA_DESTINO,
MOTIVO,
C.FECHA AS FECHA_PRE_POS,
C.CANAL AS CANAL_PRE_POS,
C.SUB_CANAL AS SUB_CANAL_PRE_POS,
C.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_PRE_POS,
C.DISTRIBUIDOR AS DISTRIBUIDOR_PRE_POS,
C.OFICINA AS OFICINA_PRE_POS,
D.FECHA AS FECHA_POS_PRE,
D.CANAL AS CANAL_POS_PRE,
D.SUB_CANAL AS SUB_CANAL_POS_PRE,
D.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_POS_PRE,
D.DISTRIBUIDOR AS DISTRIBUIDOR_POS_PRE,
D.OFICINA AS OFICINA_POS_PRE,
E.FECHA AS FECHA_CAMBIO_PLAN,
E.CANAL AS CANAL_CAMBIO_PLAN,
E.SUB_CANAL AS SUB_CANAL_CAMBIO_PLAN,
E.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_CAMBIO_PLAN,
E.DISTRIBUIDOR AS DISTRIBUIDOR_CAMBIO_PLAN,
E.OFICINA AS OFICINA_CAMBIO_PLAN,
COD_PLAN_ANTERIOR,
DES_PLAN_ANTERIOR,
TB_DESCUENTO,
TB_OVERRIDE,
DELTA
FROM ${ESQUEMA_TEMP}.${TABLA_PIVOTANTE} as Z --check!
LEFT JOIN  ${ESQUEMA_TABLA}.OTC_T_ALTA_HIST_UNIC AS A
ON (NUM_TELEFONICO=A.TELEFONO)
LEFT JOIN  ${ESQUEMA_TABLA}.OTC_T_PRE_POS_HIST_UNIC AS C
ON (NUM_TELEFONICO=C.TELEFONO)
AND (LINEA_NEGOCIO_HOMOLOGADO <>'PREPAGO')
LEFT JOIN  ${ESQUEMA_TABLA}.OTC_T_POS_PRE_HIST_UNIC AS D
ON (NUM_TELEFONICO=D.TELEFONO)
AND (LINEA_NEGOCIO_HOMOLOGADO='PREPAGO')
LEFT JOIN  ${ESQUEMA_TABLA}.OTC_T_CAMBIO_PLAN_HIST_UNIC AS E
ON (NUM_TELEFONICO=E.TELEFONO)
AND (LINEA_NEGOCIO_HOMOLOGADO <>'PREPAGO');


--CREAMOS TABLA TEMPORAL UNION PARA OBTENER ULTIMO MOVIMIENTO DEL MES POR NUM_TELEFONO
DROP TABLE ${ESQUEMA_TEMP}.OTC_T_360_PARQUE_1_MOV_MES_TMP;
CREATE TABLE ${ESQUEMA_TEMP}.OTC_T_360_PARQUE_1_MOV_MES_TMP AS 
SELECT
TIPO,
TELEFONO,
FECHA AS FECHA_MOVIMIENTO_MES,
CANAL AS CANAL_MOVIMIENTO_MES,
SUB_CANAL AS SUB_CANAL_MOVIMIENTO_MES,
NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_MOVIMIENTO_MES,
DISTRIBUIDOR AS DISTRIBUIDOR_MOVIMIENTO_MES,
OFICINA AS OFICINA_MOVIMIENTO_MES,
PORTABILIDAD AS PORTABILIDAD_MOVIMIENTO_MES,
OPERADORA_ORIGEN AS OPERADORA_ORIGEN_MOVIMIENTO_MES,
OPERADORA_DESTINO AS OPERADORA_DESTINO_MOVIMIENTO_MES,
MOTIVO AS MOTIVO_MOVIMIENTO_MES,
COD_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR_MOVIMIENTO_MES,
DES_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR_MOVIMIENTO_MES,
TB_DESCUENTO AS TB_DESCUENTO_MOVIMIENTO_MES,
TB_OVERRIDE AS TB_OVERRIDE_MOVIMIENTO_MES,
DELTA AS DELTA_MOVIMIENTO_MES 
FROM (
SELECT TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
PORTABILIDAD,
OPERADORA_ORIGEN,
OPERADORA_DESTINO,
MOTIVO,
COD_PLAN_ANTERIOR,
DES_PLAN_ANTERIOR,
TB_DESCUENTO,
TB_OVERRIDE,
DELTA,
ROW_NUMBER() OVER (PARTITION BY TELEFONO ORDER BY  FECHA DESC) AS RNUM
FROM (		
SELECT TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
CAST( NULL AS STRING) AS PORTABILIDAD,
CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
CAST( NULL AS STRING) AS OPERADORA_DESTINO,
CAST( NULL AS STRING) AS MOTIVO,
COD_PLAN_ANTERIOR,
DES_PLAN_ANTERIOR,
TB_DESCUENTO,
TB_OVERRIDE,
DELTA
FROM ${ESQUEMA_TABLA}.OTC_T_CAMBIO_PLAN_HIST_UNIC
WHERE FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}' 
UNION ALL 
SELECT
TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
CAST( NULL AS STRING) AS PORTABILIDAD,
CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
CAST( NULL AS STRING) AS OPERADORA_DESTINO,
CAST( NULL AS STRING) AS MOTIVO,
CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
CAST( NULL AS DOUBLE) AS DELTA
FROM ${ESQUEMA_TABLA}.OTC_T_POS_PRE_HIST_UNIC
WHERE FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}' 
UNION ALL 
SELECT
TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
CAST( NULL AS STRING) AS PORTABILIDAD,
CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
CAST( NULL AS STRING) AS OPERADORA_DESTINO,
CAST( NULL AS STRING) AS MOTIVO,
CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
CAST( NULL AS DOUBLE) AS DELTA
FROM ${ESQUEMA_TABLA}.OTC_T_PRE_POS_HIST_UNIC
WHERE FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}' 
UNION ALL 
SELECT
TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
PORTABILIDAD,
OPERADORA_ORIGEN,
OPERADORA_DESTINO,
MOTIVO,
CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
CAST( NULL AS DOUBLE) AS DELTA
FROM ${ESQUEMA_TABLA}.OTC_T_BAJA_HIST_UNIC
WHERE FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}' 
UNION ALL 
SELECT
TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
PORTABILIDAD,
OPERADORA_ORIGEN,
OPERADORA_DESTINO,
MOTIVO,
CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
CAST( NULL AS DOUBLE) AS DELTA
FROM ${ESQUEMA_TABLA}.OTC_T_ALTA_HIST_UNIC
WHERE FECHA  BETWEEN '${f_inicio}' AND '${fecha_proceso}' 
) ZZ ) TT
WHERE RNUM=1;
	
DROP TABLE ${ESQUEMA_TEMP}.otc_t_360_parque_1_mov_seg_tmp;	
CREATE TABLE ${ESQUEMA_TEMP}.otc_t_360_parque_1_mov_seg_tmp AS 
SELECT	TIPO AS ORIGEN_ALTA_SEGMENTO,
TELEFONO,
FECHA AS FECHA_ALTA_SEGMENTO,
CANAL AS CANAL_ALTA_SEGMENTO,
SUB_CANAL AS SUB_CANAL_ALTA_SEGMENTO,
NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA_SEGMENTO,
DISTRIBUIDOR AS DISTRIBUIDOR_ALTA_SEGMENTO,
OFICINA AS OFICINA_ALTA_SEGMENTO,
PORTABILIDAD AS PORTABILIDAD_ALTA_SEGMENTO,
OPERADORA_ORIGEN AS OPERADORA_ORIGEN_ALTA_SEGMENTO,
OPERADORA_DESTINO AS OPERADORA_DESTINO_ALTA_SEGMENTO,
MOTIVO AS MOTIVO_ALTA_SEGMENTO
FROM (
SELECT TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
PORTABILIDAD,
OPERADORA_ORIGEN,
OPERADORA_DESTINO,
MOTIVO,
ROW_NUMBER() OVER (PARTITION BY TELEFONO ORDER BY  FECHA DESC) AS RNUM
FROM (		
SELECT
TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
CAST( NULL AS STRING) AS PORTABILIDAD,
CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
CAST( NULL AS STRING) AS OPERADORA_DESTINO,
CAST( NULL AS STRING) AS MOTIVO		
FROM ${ESQUEMA_TABLA}.OTC_T_POS_PRE_HIST_UNIC
UNION ALL 
SELECT
TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
CAST( NULL AS STRING) AS PORTABILIDAD,
CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
CAST( NULL AS STRING) AS OPERADORA_DESTINO,
CAST( NULL AS STRING) AS MOTIVO
FROM ${ESQUEMA_TABLA}.OTC_T_PRE_POS_HIST_UNIC
UNION ALL 
SELECT
TIPO,
TELEFONO,
FECHA,
CANAL,
SUB_CANAL,
NUEVO_SUB_CANAL,
DISTRIBUIDOR,
OFICINA,
PORTABILIDAD,
OPERADORA_ORIGEN,
OPERADORA_DESTINO,
MOTIVO
FROM ${ESQUEMA_TABLA}.OTC_T_ALTA_HIST_UNIC
) ZZ ) TT
WHERE RNUM=1;			

DROP TABLE ${ESQUEMA_TEMP}.otc_t_360_parque_1_tmp_t_mov_mes;
CREATE TABLE ${ESQUEMA_TEMP}.otc_t_360_parque_1_tmp_t_mov_mes AS
SELECT
NUM_TELEFONICO,
CODIGO_PLAN,
FECHA_ALTA,
FECHA_LAST_STATUS,
ESTADO_ABONADO,
FECHA_PROCESO,
NUMERO_ABONADO,
LINEA_NEGOCIO,
ACCOUNT_NUM,
SUB_SEGMENTO,
TIPO_DOC_CLIENTE,
IDENTIFICACION_CLIENTE,
CLIENTE,
CUSTOMER_REF,
COUNTED_DAYS,
LINEA_NEGOCIO_HOMOLOGADO,
CATEGORIA_PLAN,
TARIFA,
NOMBRE_PLAN,
MARCA,
CICLO_FACT,
CORREO_CLIENTE_PR,
TELEFONO_CLIENTE_PR,
IMEI,
ORDEN,
TIPO_MOVIMIENTO_MES,
B.FECHA_MOVIMIENTO_MES,
ES_PARQUE,
BANCO,
CANAL_MOVIMIENTO_MES,
SUB_CANAL_MOVIMIENTO_MES,
NUEVO_SUB_CANAL_MOVIMIENTO_MES,
DISTRIBUIDOR_MOVIMIENTO_MES,
OFICINA_MOVIMIENTO_MES,
PORTABILIDAD_MOVIMIENTO_MES,
OPERADORA_ORIGEN_MOVIMIENTO_MES,
OPERADORA_DESTINO_MOVIMIENTO_MES,
MOTIVO_MOVIMIENTO_MES,
COD_PLAN_ANTERIOR_MOVIMIENTO_MES,
DES_PLAN_ANTERIOR_MOVIMIENTO_MES,
TB_DESCUENTO_MOVIMIENTO_MES,
TB_OVERRIDE_MOVIMIENTO_MES,
DELTA_MOVIMIENTO_MES
FROM ${ESQUEMA_TEMP}.${TABLA_PIVOTANTE} AS B
LEFT JOIN  ${ESQUEMA_TEMP}.OTC_T_360_PARQUE_1_MOV_MES_TMP AS A
ON (NUM_TELEFONICO=A.TELEFONO)
AND B.FECHA_MOVIMIENTO_MES=A.FECHA_MOVIMIENTO_MES;