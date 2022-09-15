------------------------------------------------------
--EJECUCION DE CONSULTA EN HIVE (INSERTAR QUERY)
------------------------------------------------------

set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		
				insert overwrite table db_desarrollo2021.otc_t_360_general partition(fecha_proceso)
				select distinct 
				t1.telefono
					,t1.codigo_plan
					,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(pp.usa_app,'NO') ELSE 'NO' END) as usa_app
					,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(pp.usuario_app,'NO') ELSE 'NO' END) as usuario_app
					,t1.usa_movistar_play
					,t1.usuario_movistar_play						
					,t1.fecha_alta
					,t1.nse
					,t1.sexo
					,t1.edad
					,t1.mes
					,t1.anio
					,t1.segmento
					,t1.linea_negocio
					,t1.linea_negocio_homologado
					,t1.forma_pago_factura
					,t1.forma_pago_alta
					,t1.estado_abonado
					,t1.sub_segmento					
					,t1.numero_abonado
					,t1.account_num
					,t1.identificacion_cliente
					,t1.customer_ref
					,t1.tac
					,t1.tiene_bono
					,t1.valor_bono
					,t1.codigo_bono
					,t1.probabilidad_churn
					,t1.counted_days
					,t1.categoria_plan
					,t1.tarifa
					,t1.nombre_plan
					,t1.marca
					,t1.grupo_prepago
					,t1.fidelizacion_megas
					,t1.fidelizacion_dumy
					,t1.bancarizado
					,nvl(t1.bono_combero,'') as bono_combero
					,t1.ticket_recarga		
          ,nvl(t1.tiene_score_tiaxa,'NO') as tiene_score_tiaxa
          ,t1.score_1_tiaxa
           ,t1.score_2_tiaxa			
		  ,t1.tipo_doc_cliente 
		  ,t1.cliente as nombre_cliente
		  ,t1.ciclo_fact as ciclo_facturacion
		  ,t1.email
		  ,t1.telefono_contacto
		  ,t1.fecha_ultima_renovacion
		  ,t1.address_2
		  ,t1.address_3
		  ,t1.address_4
		  ,t1.fecha_fin_contrato_definitivo
		  ,t1.vigencia_contrato
		  ,t1.version_plan
		  ,t1.fecha_ultima_renovacion_jn
		  ,t1.fecha_ultimo_cambio_plan
		  ,t1.tipo_movimiento_mes
		  ,t1.fecha_movimiento_mes
		  ,t1.es_parque
		  ,t1.banco			   
		  ,t1.parque_recargador			
		,t1.segmento_fin as segmento_parque
		  ,t1.susp_cobranza
		  ,t1.susp_911
		  ,t1.susp_cobranza_puntual
		  ,t1.susp_fraude
		  ,t1.susp_robo
		  ,t1.susp_voluntaria
		  ,t1.vencimiento_cartera
		  ,t1.saldo_cartera
		,A2.fecha_alta_historica	
		,A2.CANAL_ALTA
		,A2.SUB_CANAL_ALTA
		--,A2.NUEVO_SUB_CANAL_ALTA
		,A2.DISTRIBUIDOR_ALTA
		,A2.OFICINA_ALTA
		,A2.PORTABILIDAD
		,A2.OPERADORA_ORIGEN
		,A2.OPERADORA_DESTINO
		,A2.MOTIVO
		,A2.FECHA_PRE_POS
		,A2.CANAL_PRE_POS
		,A2.SUB_CANAL_PRE_POS
		--,A2.NUEVO_SUB_CANAL_PRE_POS
		,A2.DISTRIBUIDOR_PRE_POS
		,A2.OFICINA_PRE_POS
		,A2.FECHA_POS_PRE
		,A2.CANAL_POS_PRE
		,A2.SUB_CANAL_POS_PRE
		--,A2.NUEVO_SUB_CANAL_POS_PRE
		,A2.DISTRIBUIDOR_POS_PRE
		,A2.OFICINA_POS_PRE
		,A2.FECHA_CAMBIO_PLAN
		,A2.CANAL_CAMBIO_PLAN
		,A2.SUB_CANAL_CAMBIO_PLAN
		--,A2.NUEVO_SUB_CANAL_CAMBIO_PLAN
		,A2.DISTRIBUIDOR_CAMBIO_PLAN
		,A2.OFICINA_CAMBIO_PLAN
		,A2.COD_PLAN_ANTERIOR
		,A2.DES_PLAN_ANTERIOR
		,A2.TB_DESCUENTO
		,A2.TB_OVERRIDE
		,A2.DELTA
		,A1.CANAL_MOVIMIENTO_MES
		,A1.SUB_CANAL_MOVIMIENTO_MES
		--,A1.NUEVO_SUB_CANAL_MOVIMIENTO_MES
		,A1.DISTRIBUIDOR_MOVIMIENTO_MES
		,A1.OFICINA_MOVIMIENTO_MES
		,A1.PORTABILIDAD_MOVIMIENTO_MES
		,A1.OPERADORA_ORIGEN_MOVIMIENTO_MES
		,A1.OPERADORA_DESTINO_MOVIMIENTO_MES
		,A1.MOTIVO_MOVIMIENTO_MES
		,A1.COD_PLAN_ANTERIOR_MOVIMIENTO_MES
		,A1.DES_PLAN_ANTERIOR_MOVIMIENTO_MES
		,A1.TB_DESCUENTO_MOVIMIENTO_MES
		,A1.TB_OVERRIDE_MOVIMIENTO_MES
		,A1.DELTA_MOVIMIENTO_MES
		,A3.Fecha_Alta_Cuenta
		,t1.fecha_inicio_pago_actual
		,t1.fecha_fin_pago_actual
		,t1.fecha_inicio_pago_anterior
		,t1.fecha_fin_pago_anterior
		,t1.forma_pago_anterior
		,A4.origen_alta_segmento
		,A4.fecha_alta_segmento
		,A5.dias_voz
		,A5.dias_datos
		,A5.dias_sms
		,A5.dias_conenido
		,A5.dias_total
		,t1.limite_credito
		,cast(p1.adendum as double)
		--,cast(t1.fecha_proceso as bigint) fecha_proceso
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.fecha_registro_app ELSE NULL END) as fecha_registro_app
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.perfil ELSE 'NO' END) as perfil
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(wb.usuario_web,'NO') ELSE 'NO' END) as usuario_web
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN wb.fecha_registro_web ELSE NULL END) as fecha_registro_web
		--20210629 - SE AGREGA CAMPO FECHA NACIMIENTO
		--20210712 - Giovanny Cholca, valida que la fecha actual - fecha de nacimiento no sea menor a 18 aÃ±os, si se cumple colocamos null al a la fecha de nacimiento
		,case when round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) <18 
		or round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) > 120
		then null else cs.fecha_nacimiento end as fecha_nacimiento
		,${FECHAEJE} as fecha_proceso
		FROM ${ESQUEMA_TEMP}.otc_t_360_general_temp_final_rf t1
		LEFT JOIN db_temporales.otc_t_360_parque_1_tmp_t_mov A2 ON (t1.TELEFONO=A2.NUM_TELEFONICO) AND (t1.LINEA_NEGOCIO=a2.LINEA_NEGOCIO)
		LEFT JOIN  db_temporales.OTC_T_360_PARQUE_1_MOV_MES_TMP A1 ON (t1.TELEFONO=A1.TELEFONO) AND (t1.fecha_movimiento_mes=A1.fecha_movimiento_mes)
		LEFT JOIN db_temporales.otc_t_cuenta_num_tmp A3 ON (t1.account_num=A3.cta_fact)
		LEFT JOIN db_temporales.otc_t_360_parque_1_mov_seg_tmp A4 ON (t1.TELEFONO=A4.TELEFONO) AND (t1.es_parque='SI')
		LEFT JOIN db_temporales.OTC_T_parque_traficador_dias_tmp A5 ON (t1.TELEFONO=A5.TELEFONO) AND (${FECHAEJE}=A5.fecha_corte)
		LEFT JOIN db_temporales.otc_t_360_general_temp_adendum p1 ON (t1.TELEFONO=p1.phone_number)
		LEFT JOIN db_temporales.tmp_360_app_mi_movistar pp ON (t1.telefono=pp.num_telefonico)
		LEFT JOIN db_temporales.tmp_360_web wb ON (t1.customer_ref=wb.cust_ext_ref)
		--20210629 - SE REALIZA EL CRUCE CON LA TEMPORAL PARA AGREGAR CAMPO FECHA NACIMIENTO
		LEFT JOIN db_temporales.tmp_fecha_nacimiento_mvp cs ON t1.identificacion_cliente=cs.cedula;




