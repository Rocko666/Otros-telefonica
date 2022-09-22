--                                          PARAMS PARA CATALOGO_CONSOLIDADO
--CAMBIAR LA ENTIDAD
DELETE FROM params_des WHERE entidad='OTC_T_CATALOGO_CONSOLIDADO_ID';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_USER','nae108834',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_RUTA','/home/nae108834/Cliente360_RF',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_FTP_RUTA','/CATALOGOS/COMISIONES/EXT_MOVIMIENTOS',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_FTP_PUERTO','9156',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_FTP_HOSTNAME','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_FTP_PASS','.R{53Hk%M{|kd]9',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_FTP_NOM_ARCHIVO','ConsolidadoID.xlsx',0,0);
insert into params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE)VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_DIR_HDFS_CAT','db_desarrollo2021.OTC_T_CATALOGO_CONSOLIDADO_ID',1,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_CATALOGO_CONSOLIDADO_ID','VAL_FTP_USER','ftp_reportes',0,0);
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_CATALOGO_CONSOLIDADO_ID';

---------------------------------------------------------

DELETE FROM params_des WHERE entidad='EXCEL_PRUEBA';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_RUTA','/home/nae108834/tabla_360',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_FTP_RUTA','/Admventas/COMISIONES/Indirectos/Multipais/PROYECTO_COMISIONES/MOVIMIENTOS',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_FTP_PUERTO','9664',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_FTP_HOSTNAME','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_FTP_PASS','NA2022-04-bi',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_FTP_NOM_ARCHIVO','ConsolidadoID.xlsx',0,0);
insert into params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE)VALUES('EXCEL_PRUEBA','VAL_DIR_HDFS_CAT','/DataExternal/db_desarrollo2021/catalogo_consolidado',1,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_FTP_USER','NABIFI01',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('EXCEL_PRUEBA','VAL_FTP_NOM_ARCHIVO_2','CTL_POS_USR_NC.xls',0,0);


--                                          PARAMS 360 MOVI PARQUE REFACTORING
DELETE FROM params_des WHERE entidad='D_OTC_T_360_MOVIMIENTOS_PARQUE';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_MOVIMIENTOS_PARQUE','VAL_USER','nae108834',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_MOVIMIENTOS_PARQUE','VAL_SQL','D_OTC_T_360_MOVIMIENTOS_PARQUE.sql',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_MOVIMIENTOS_PARQUE','VAL_RUTA','/home/nae108834/Cliente360_RF',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_MOVIMIENTOS_PARQUE','SHELL','/home/nae108834/Cliente360_RF/bin/D_OTC_T_360_MOVIMIENTOS_PARQUE_RF.sh',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_MOVIMIENTOS_PARQUE','ESQUEMA_TEMP','db_desarrollo2021',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_MOVIMIENTOS_PARQUE','ESQUEMA_TABLA','db_desarrollo2021',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_MOVIMIENTOS_PARQUE','TABLA_PIVOTANTE','otc_t_360_parque_1_tmp',0,0);
SELECT * FROM params_des WHERE ENTIDAD='D_OTC_T_360_MOVIMIENTOS_PARQUE';


--PARAMS 360 GENERAL REFACTORING
DELETE FROM params_des WHERE entidad='D_OTC_T_360_GENERAL';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_USER','nae108834',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_RUTA','/home/nae108834/Cliente360_RF',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','SHELL','/home/nae108834/Cliente360_RF/bin/D_OTC_T_360_GENERAL.sh',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_SQL_1','D_OTC_T_360_GENERAL_1.sql',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_SQL_2','D_OTC_T_360_GENERAL_2.sql',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TOPE_RECARGAS','45',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TOPE_TARIFA_BASICA','45',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','PESOS_PARAMETROS','0.45;0.05;0.40;0.04;0.02;0.02;0.02',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','PESOS_NSE','0.80;0.60;0.40;0.20;0.00',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','ESQUEMA_TABLA','db_thebox',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','ESQUEMA_TABLA_1','db_files_novum',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','ESQUEMA_TABLA_2','db_thebox',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','ESQUEMA_TABLA_3','db_payment_manager',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_FTP_RUTA','/CATALOGOS/COMISIONES/EXT_MOVIMIENTOS',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_FTP_PUERTO','9156',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_FTP_USER','ftp_reportes',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_FTP_HOSTNAME','fileuio03',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_FTP_PASS','.R{53Hk%M{|kd]9',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_FTP_NOM_ARCHIVO','ConsolidadoID.xlsx',0,0);
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','VAL_DIR_HDFS_CAT','db_desarrollo2021.OTC_T_CATALOGO_CONSOLIDADO_ID',1,0);
--Para SQOOP
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TDUSER','rdb_reportes','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TDPASS','TelfEcu2017','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TDHOST','proxfulldg1.otecel.com.ec','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TDPORT','7594','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TDDB','tomstby.otecel.com.ec','0','0');
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TDUSER','naaabatch','0','0');
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_GENERAL','TDPASS','only_11g','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_OTC_T_360_GENERAL';



-------PARAMS PARA EXTRACTOR DE MOVIMIENTOS
DELETE FROM params_des WHERE entidad='D_EXTRACTOR_DE_MOVIMIENTOS';
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_RUTA','/home/nae108834/SPARK_EXTRACTOR_DE_MOVIMIENTOS',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_FTP_RUTA_OUT','/CATALOGOS/COMISIONES/EXT_MOVIMIENTOS',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_FTP_USER_OUT','ftp_reportes',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_FTP_PUERTO_OUT','9156',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_FTP_HOSTNAME_OUT','fileuio03',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_FTP_PASS_OUT','.R{53Hk%M{|kd]9',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_FTP_NOM_ARCHIVO','extractor_movimientos.xlsx',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_RUTA_IN','/home/nae108834/SPARK_EXTRACTOR_DE_MOVIMIENTOS/Input',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_RUTA_APLICACION','/usr/hdp/current/spark2-client/bin/spark-submit',0,0);
--insert into params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE)VALUES('D_EXTRACTOR_DE_MOVIMIENTOS','VAL_DIR_HDFS_CAT','db_desarrollo2021.OTC_T_CATALOGO_CONSOLIDADO_ID',1,0);

SELECT * FROM params_des WHERE ENTIDAD='D_EXTRACTOR_DE_MOVIMIENTOS';



$VAL_ARCH_EXTRCTR


