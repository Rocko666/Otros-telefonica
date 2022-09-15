CREATE TABLE db_desarrollo2021.otc_t_360_general(
  num_telefonico string, 
  usuario_web string, 
  fecha_registro_web timestamp, 
  fecha_nacimiento date COMMENT 'Fecha de nacimiento del cliente en formato YYYY-MM-DD')
PARTITIONED BY ( 
  fecha_proceso bigint)

STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://quisrvbigdataha/apps/hive/warehouse/db_desarrollo2021.db/otc_t_360_general'
TBLPROPERTIES (
  'orc.compress'='SNAPPY', 
  'transient_lastDdlTime'='1644361593')

beeline -u 'jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2?tez.queue.name=default' -n nae105215 --hiveconf tez.queue.name=default --hiveconf hive.auto.convert.sortmerge.join=true --hiveconf hive.optimize.bucketmapjoin=true --hiveconf hive.optimize.bucketmapjoin.sortedmerge=true --hivevar ESQUEMA_TEMP=db_desarrollo2021 --hivevar fechamas1=20220616 --hivevar FECHAEJE=20220615 --hivevar fechamenos1mes=20220515 --hivevar fechamenos2mes=20220415 --hivevar ESQUEMA_TABLA_1=db_files_novum --hivevar ESQUEMA_TABLA_3=db_payment_manager --hivevar ESQUEMA_TABLA_2=db_thebox -f /home/nae108834/tabla_360/sql/hql_2.sql