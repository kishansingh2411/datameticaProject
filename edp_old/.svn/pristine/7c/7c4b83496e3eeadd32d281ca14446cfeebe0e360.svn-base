--######################################################################################################################
--#   this source code is the property of:
--#   cablevision systems corporation, inc. (c) 2015
--#   1111 stewart avenue, bethpage, ny 11714
--#   www.cablevision.com
--#   department: am
--#
--#   program name: module-transform.hql
--#   program type: hive query language script
--#   purpose:    : build work_rsdvr_requests_archive_dedup_tbl table 
--#   author(s)   : DataMetica Team
--#   usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   date        : 09/12/2016
--#   log file    : .../log/mrdvr/${job_name}.log
--#   sql file    : 
--#   error file  : .../log/mrdvr/${job_name}.log
--#   dependency  : 
--#   disclaimer  : 
--#
--#   modification history :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2015       initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE TABLE ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_requests_archive_dedup_tbl}
SELECT 
	schedule_id_incoming           ,
	call_sign_incoming            ,
	duration_incoming             ,
	end_time_adjustment_incoming  ,
	home_id_incoming              ,
	mac_address_incoming          ,
	serial_number_incoming        ,
	create_time_incoming          ,
	modified_time_incoming        ,
	recording_type_incoming       ,
	save_all_incoming             ,
	save_latest_incoming          ,
	start_date_incoming           ,
	start_time_incoming           ,
	save_days_incoming            ,
	status_incoming               ,
	short_title_incoming          ,
	archive_time_incoming         ,
	archive_date_est_incoming     ,
	source_load_date_incoming              ,
	schedule_id_gold               ,
	call_sign_gold                 ,
	duration_gold                  ,
	end_time_adjustment_gold       ,
	home_id_gold                   ,
	mac_address_gold               ,
	create_time_gold               ,
	modified_time_gold             ,
	recording_type_gold            ,
	save_all_gold                  ,
	save_latest_gold               ,
	serial_number_gold             ,
	start_date_gold                ,
	start_time_gold                ,
	save_days_gold    ,
	status_gold                    ,
	short_title_gold               ,
	archive_date_est_gold          ,
	source_load_date_gold          ,
	op_type_gold                   ,
	mac_address_ind                ,
	call_sign_ind                  ,
	start_date_ind                 ,
	start_time_ind                 ,
	duration_ind                   ,
	recording_type_ind             ,
	save_all_ind                   ,
	save_latest_ind                ,
	create_time_ind                ,
	modified_time_ind              ,
	end_time_adjustment_ind        ,
	save_days_ind                ,
	status_ind                   ,
	short_title_ind              ,
	archive_date_est_ind         ,
	source_load_date_ind         ,
	newflag                        ,
	changedflag                    ,
	load_date                      ,
	dtm_sysdate                    ,
	y_ind                          ,
	op_type_i 					,
	op_type_u 					,
	rsdvr_requests_archive_delta_seq
FROM 
(SELECT 
	seq.*,
	ROW_NUMBER () OVER (PARTITION BY schedule_id_incoming,home_id_incoming,serial_number_incoming ORDER BY modified_time_incoming DESC ) AS RECORD_ROW_NUM
FROM 
	${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_requests_archive_seq_tbl} seq
WHERE 
	CHANGEDFLAG = 'TRUE' OR NEWFLAG='TRUE'
)dedup
WHERE 
	dedup.RECORD_ROW_NUM = 1
;
###############################################################################
#                                     End                                     #
###############################################################################