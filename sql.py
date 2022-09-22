from Funciones.funcion import *

@cargar_consulta
def fun_extraer_datos_mecorig(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,(sum(coste/1000.000)/1.12) od_sms
           ,count(*) cantidad
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada not in ('PQTFEE','PQTNET','SOSFEE','SOSNET')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_datos_llamadas(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,(sum(coste/1000.000)/1.12) od_voz
           ,(sum(duracion_total/60)) cant_minutos
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada not in ('PQTFEE','PQTNET','SOSFEE','SOSNET')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_datos_contenidos(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,sum(coste/1000)/1.12 cobrado
           ,count (*) cantidad_eventos
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada not in ('PQTFEE','PQTNET','SOSFEE','SOSNET')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_datos_adelanto_saldo(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,sum(coste/1000)/1.12 cobrado
           ,count (*) cantidad_eventos
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada in ('SOSFEE','PQTFEE')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry
  
@cargar_consulta
def fun_extraer_datos_buzon_voz_diario(bdd, tabla, fecha_inicial, fecha_final, fec_cambio_buzon, fec_cambio_buzon_co, codigo_act, codigo_us):
    qry = '''
       SELECT 
           num_telefono
           ,FECHA
           ,COD_TIPPREPA
           ,sum(imp_coste) as valor
           ,count(*) as cantidad
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and cod_actuacio = (case 
				when fecha <= {fec_cambio_buzon} and cod_usuario='OLYMPUS' then 'SE'
	  			when (fecha > {fec_cambio_buzon} AND fecha < {fec_cambio_buzon_co})  and cod_usuario='BUZONVOZ' then '9A'
				when fecha >= {fec_cambio_buzon_co} and cod_usuario='{codigo_us}' then '{codigo_act}' end)
           and cod_estarec = 'EJ'
           and imp_coste = 18
           and cod_usuario in ('OLYMPUS','{codigo_us}')
           group by num_telefono
            ,FECHA
            ,COD_TIPPREPA
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final, fec_cambio_buzon=fec_cambio_buzon,fec_cambio_buzon_co=fec_cambio_buzon_co, codigo_act=codigo_act, codigo_us=codigo_us)
    return qry



