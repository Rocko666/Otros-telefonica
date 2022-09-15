--------------------------------------------------------------------------------------------------------
-- NOMBRE: otc_t_movimientos.sql
-- DESCRIPCION:
--   HQL que ejecuta el drop y creacion de la tabla  OTC_T_ALTAS_BI
-- AUTOR: Cristian Ortiz - Softconsulting
-- FECHA CREACION: 2022-06-20 
--------------------------------------------------------------------------------------------------------
-- MODIFICACIONES
-- FECHA         AUTOR                      DESCRIPCION MOTIVO
-- YYYY-MM-DD    NOMBRE Y APELLIDO          MOTIVO DEL CAMBIO
--------------------------------------------------------------------------------------------------------
--SET VARIABLES
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET p_fecha=${hivevar:p_fecha};
SET ini_fecha=${hivevar:init_fecha};

--EJECUTA EL DROP DE LA TABLA
DROP TABLE IF EXISTS db_desarrollo2021.otc_t_movimientos;

CREATE TABLE `db_desarrollo2021.otc_t_movimientos`(
  id_producto int,
  linea_negocio varchar(60), 
  telefono char(9), 
  numero_abonado char(8), 
  account_num char(8), 
  fecha_movimiento date, 
  estado_abonado char(3), 
  cliente varchar(210), 
  documento_cliente char(13), 
  tipo_doc_cliente varchar(60), 
  plan_codigo varchar(20), 
  nombre_plan varchar(40), 
  ciudad varchar(110), 
  provincia varchar(60), 
  imei char(14), 
  equipo string, 
  icc char(19), 
  sub_segmento varchar(60), 
  segmento varchar(20), 
  segmento_fin varchar(30), 
  fecha_proceso date, 
  tarifa_basica_actual float, 
  categoria_plan varchar(10), 
  cod_categoria varchar(20), 
  domain_login_ow varchar(110), 
  nombre_usuario_ow varchar(110), 
  domain_login_sub varchar(110), 
  nombre_usuario_sub varchar(110), 
  canal varchar(110), 
  distribuidor varchar(110), 
  oficina varchar(110), 
  portabilidad_alta varchar(10),
  forma_pago varchar(50), 
  cod_da string, 
  nom_usuario string,
  id_canal int,
  canal_comercial string, 
  campania string, 
  codigo_distribuidor varchar(60), 
  nom_distribuidor string, 
  codigo_plaza varchar(60), 
  nom_plaza varchar(110), 
  region string, 
  provincia_ivr string, 
  operadora_origen varchar(20), 
  ejecutivo_asignado_ptr string, 
  area_ptr string, 
  codigo_vendedor_da_ptr string, 
  jefatura_ptr string, 
  nom_email string, 
  provincia_ms string,  
  id_sub_canal int, 
  sub_canal varchar(50), 
  overwrite float,
  descuento float,
  codigo_usuario char(9), 
  marca string, 
  tecno_dispositivo char(2),
  descripcion_del_despacho varchar(110), 
  calf_riesgo char(1), 
  cap_endeu char(1), 
  valor_cred int,
  fecha_movimiento_baja date,
  movimiento_baja varchar(50),
  dias_transcurridos_baja int,
  fecha_cambio date,
  linea_de_negocio_anterior string,
  dias_en_parque int,
  id_tipo_movimiento int,
  tipo_movimiento string,
  codigo_plan_anterior varchar(10),
  cliente_anterior string,
  nombre_plan_anterior varchar(50),
  tarifa_basica_anterior float,
  fecha_inicio_plan_anterior date,
  delta_tarifa float,
  dias_reciclaje int,
  tipo_descuento string,
  tipo_descuento_conadis string,
  dias_en_parque_prepago int,
  vol_invol string,
  tarifa_basica_baja float,
  portabilidad_baja string,
  operadora_destino string,
  account_num_anterior varchar(30),
  ciudad_usuario varchar(40),
  provincia_usuario varchar(40),
  mismo_cliente char(2),
  fecha_alta_prepago date,
  tarifa_plan_actual_ov float,
  fecha_baja_reciclada date,
  tarifa_final_plan_act float,
  tarifa_final_plan_ant float,
  delta_tarifa_final float,
  tipo_cuenta_en_operador_donante string,
  idHash varchar(32),
  )
  

PARTITIONED BY ( 
  p_fecha_proceso int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
);

--CREATE EXTERNAL TABLE IF NOT EXISTS id_producto
--CREATE EXTERNAL TABLE IF NOT EXISTS id_canal
--CREATE EXTERNAL TABLE IF NOT EXISTS id_subcanal
--CREATE EXTERNAL TABLE IF NOT EXISTS id_tipo_movimientO
