--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform_delete.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build gold_requests table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform_delete.hql
--#   Date        : 09/06/2016
--#   Log File    : .../log/mrdvr/MRDVR_MOVE_TO_GOLD_JOB.log
--#   SQL File    : 
--#   Error File  : .../log/mrdvr/MRDVR_MOVE_TO_GOLD_JOB.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          09/12/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_requests_delta} 
	PARTITION (LOAD_DATE)
SELECT 
GOLD_RSDVR_REQUESTS_LAST_REC.SCHEDULE_ID              
,GOLD_RSDVR_REQUESTS_LAST_REC.HOME_ID                  
,GOLD_RSDVR_REQUESTS_LAST_REC.SERIAL_NUMBER            
,GOLD_RSDVR_REQUESTS_LAST_REC.MAC_ADDRESS              
,GOLD_RSDVR_REQUESTS_LAST_REC.CALL_SIGN                
,GOLD_RSDVR_REQUESTS_LAST_REC.START_DATE               
,GOLD_RSDVR_REQUESTS_LAST_REC.START_TIME               
,GOLD_RSDVR_REQUESTS_LAST_REC.DURATION                 
,GOLD_RSDVR_REQUESTS_LAST_REC.RECORDING_TYPE           
,GOLD_RSDVR_REQUESTS_LAST_REC.SAVE_ALL                 
,GOLD_RSDVR_REQUESTS_LAST_REC.SAVE_LATEST              
,GOLD_RSDVR_REQUESTS_LAST_REC.CREATE_TIME              
,GOLD_RSDVR_REQUESTS_LAST_REC.MODIFIED_TIME            
,GOLD_RSDVR_REQUESTS_LAST_REC.END_TIME_ADJUSTMENT      
,GOLD_RSDVR_REQUESTS_LAST_REC.SAVE_DAYS                
,GOLD_RSDVR_REQUESTS_LAST_REC.STATUS                   
,GOLD_RSDVR_REQUESTS_LAST_REC.SHORT_TITLE              
,GOLD_RSDVR_REQUESTS_LAST_REC.DS_STARTTIME             
,GOLD_RSDVR_REQUESTS_LAST_REC.LAST_UPDATED             
,GOLD_RSDVR_REQUESTS_LAST_REC.MAC_ADDRESS_IND          
,GOLD_RSDVR_REQUESTS_LAST_REC.CALL_SIGN_IND            
,GOLD_RSDVR_REQUESTS_LAST_REC.START_DATE_IND           
,GOLD_RSDVR_REQUESTS_LAST_REC.START_TIME_IND           
,GOLD_RSDVR_REQUESTS_LAST_REC.DURATION_IND             
,GOLD_RSDVR_REQUESTS_LAST_REC.RECORDING_TYPE_IND       
,GOLD_RSDVR_REQUESTS_LAST_REC.SAVE_ALL_IND             
,GOLD_RSDVR_REQUESTS_LAST_REC.SAVE_LATEST_IND          
,GOLD_RSDVR_REQUESTS_LAST_REC.MODIFIED_TIME_IND        
,GOLD_RSDVR_REQUESTS_LAST_REC.END_TIME_ADJUSTMENT_IND  
,GOLD_RSDVR_REQUESTS_LAST_REC.SAVE_DAYS_IND            
,GOLD_RSDVR_REQUESTS_LAST_REC.STATUS_IND               
,GOLD_RSDVR_REQUESTS_LAST_REC.SHORT_TITLE_IND          
,GOLD_RSDVR_REQUESTS_LAST_REC.DS_STARTTIME_IND         
,GOLD_RSDVR_REQUESTS_LAST_REC.LAST_UPDATED_IND         
,GOLD_RSDVR_REQUESTS_LAST_REC.CREATE_TIME_IND          
,WORK_RSDVR_REQUESTS_DELETE_DEDUP.OP_TYPE                  
,WORK_RSDVR_REQUESTS_DELETE_DEDUP.DTM_CREATED              
,GOLD_RSDVR_REQUESTS_LAST_REC.RSDVR_REQUESTS_DELTA_SEQ 
,DATE_FORMAT('${hivevar:source_date}','yyyy-MM-dd') as LOAD_DATE
FROM 
	${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_requests_last_rec} GOLD_RSDVR_REQUESTS_LAST_REC INNER JOIN ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_requests_delete_dedup} WORK_RSDVR_REQUESTS_DELETE_DEDUP 
ON
	(GOLD_RSDVR_REQUESTS_LAST_REC.SCHEDULE_ID = WORK_RSDVR_REQUESTS_DELETE_DEDUP.SCHEDULE_ID AND
	GOLD_RSDVR_REQUESTS_LAST_REC.HOME_ID = WORK_RSDVR_REQUESTS_DELETE_DEDUP.HOME_ID AND
	GOLD_RSDVR_REQUESTS_LAST_REC.SERIAL_NUMBER = WORK_RSDVR_REQUESTS_DELETE_DEDUP.SERIAL_NUMBER
	)
WHERE
	GOLD_RSDVR_REQUESTS_LAST_REC.RSDVR_REQUESTS_DELTA_SEQ=WORK_RSDVR_REQUESTS_DELETE_DEDUP.RSDVR_REQUESTS_DELTA_SEQ		
;