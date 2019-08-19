--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Work Tmp table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
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
--#    1.0     DataMetica Team          09/06/2016       Initial version
--#
--#
--#####################################################################################################################
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE 
   ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_recordings_archive_delta_tbl}
   PARTITION (load_date)
SELECT
   ASSET_ID_INCOMING AS ASSET_ID	                   
   ,CALL_SIGN_INCOMING AS CALL_SIGN	               
   ,CALL_SIGN_IND	           
   ,CREATE_TIME_INCOMING AS CREATE_TIME	               
   ,CREATE_TIME_IND	           
   ,STARTDATE_STARTTIME_EST_INCOMING AS STARTDATE_STARTTIME_EST	               
   ,STARTDATE_STARTTIME_EST_IND	           
   ,ARCHIVE_TIME_INCOMING AS ARCHIVE_TIME	           
   ,ARCHIVE_TIME_IND	       
   ,DTM_SYSDATE AS DTM_CREATED                
   ,DURATION_INCOMING AS DURATION	               
   ,DURATION_IND	           
   ,ENDTIME_ADJUSTMENT_PREV_INCOMING AS ENDTIME_ADJUSTMENT_PREV	   
   ,ENDTIME_ADJUSTMENT_PREV_IND
   ,ENDTIME_ADJUSTMENT_INCOMING       AS ENDTIME_ADJUSTMENT	       
   ,ENDTIME_ADJUSTMENT_IND      
   ,HOME_ID_INCOMING                 AS HOME_ID	                   
   ,KEEP_TYPE_CODE_INCOMING          AS KEEP_TYPE_CODE	           
   ,KEEP_TYPE_CODE_IND	       
   ,ARCHIVE_DATE_EST_INCOMING            AS ARCHIVE_DATE_EST	           
   ,ARCHIVE_DATE_EST_IND	       
   ,SOURCE_LOAD_DATE_INCOMING                AS SOURCE_LOAD_DATE	               
   ,SOURCE_LOAD_DATE_IND
   ,STARTDATE_EST_INCOMING AS STARTDATE_EST
   ,STARTDATE_EST_IND	           
   ,MODIFY_TIME_INCOMING             AS MODIFY_TIME	               
   ,MODIFY_TIME_IND	           	                   
   ,CASE 
      WHEN CHANGEDFLAG='TRUE' THEN OP_TYPE_U
      WHEN NEWFLAG='TRUE' THEN OP_TYPE_I
   END                               AS OP_TYPE
   ,PAUSE_TIME_INCOMING              AS PAUSE_TIME	               
   ,PAUSE_TIME_IND	           
   ,RECORDING_DATE_INCOMING          AS RECORDING_DATE	           
   ,RECORDING_DATE_IND	       
   ,RECORDING_ID_INCOMING            AS RECORDING_ID	           
   ,RECORDING_LOCKED_INCOMING        AS RECORDING_LOCKED	       
   ,RECORDING_LOCKED_IND	   
   ,RECORDING_STATUS_INCOMING        AS RECORDING_STATUS	       
   ,RECORDING_STATUS_IND	   
   ,RECORDING_TIME_INCOMING          AS RECORDING_TIME	           
   ,RECORDING_TIME_IND	       
   ,RSDVR_RECORDINGS_DELTA_SEQ	
   ,SCHEDULE_ID_INCOMING             AS SCHEDULE_ID	               
   ,START_DATE_INCOMING              AS START_DATE	               
   ,START_DATE_IND	           
   ,START_TIME_INCOMING              AS START_TIME	               
   ,START_TIME_IND             
   ,STARTDATE_STARTTIME_INCOMING     AS STARTDATE_STARTTIME	       
   ,STARTDATE_STARTTIME_IND	   
   ,TEMPORARY_ASSET_ID_INCOMING      AS TEMPORARY_ASSET_ID	       
   ,TEMPORARY_ASSET_ID_IND	   
   ,DATE_FORMAT(LOAD_DATE,'yyyy-MM-dd') AS LOAD_DATE
FROM
   ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_recordings_archive_dedup_tbl}
WHERE
   NEWFLAG='TRUE' 
   OR CHANGEDFLAG='TRUE'
;
--##############################################################################
--#                                    End                                     #
--##############################################################################