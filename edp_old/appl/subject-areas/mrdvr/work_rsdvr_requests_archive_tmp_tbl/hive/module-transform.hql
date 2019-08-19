--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2016
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build work_rsdvr_requests_archive_tmp_tbl table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/mrdvr/${job_name}.log
--#   SQL File    :
--#   Error File  : .../log/mrdvr/${job_name}.log
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

INSERT OVERWRITE TABLE ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_requests_archive_tmp_tbl}
SELECT
INCOMING_RSDVR_REQUESTS_ARCHIVE.ID_SCHEDULE as SCHEDULE_ID_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.CALLSIGN as CALL_SIGN_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.DURATION as DURATION_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.ENDTIME_ADJUSTMENT as END_TIME_ADJUSTMENT_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.ID_HOME as HOME_ID_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.MACADDRESS as MAC_ADDRESS_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.SERIALNUM as SERIAL_NUMBER_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.CREATE_TIME as CREATE_TIME_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.MODIFY_TIME as MODIFIED_TIME_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.REC_TYPE as RECORDING_TYPE_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_ALL as SAVE_ALL_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_LATEST as SAVE_LATEST_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.STARTDATE as START_DATE_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.STARTTIME as START_TIME_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_DAYS as SAVE_DAYS_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.STATUS STATUS_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.SHORT_TITLE as SHORT_TITLE_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME as ARCHIVE_TIME_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_DATE_EST as ARCHIVE_DATE_EST_INCOMING,
INCOMING_RSDVR_REQUESTS_ARCHIVE.SOURCE_LOAD_DATE as SOURCE_LOAD_DATE_INCOMING,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SCHEDULE_ID as SCHEDULE_ID_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.CALL_SIGN as CALL_SIGN_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.DURATION as DURATION_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.END_TIME_ADJUSTMENT as END_TIME_ADJUSTMENT_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.HOME_ID as HOME_ID_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.MAC_ADDRESS as MAC_ADDRESS_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.CREATE_TIME as CREATE_TIME_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.MODIFIED_TIME as MODIFIED_TIME_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.RECORDING_TYPE as RECORDING_TYPE_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_ALL as SAVE_ALL_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_LATEST as SAVE_LATEST_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SERIAL_NUMBER as SERIAL_NUMBER_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.START_DATE as START_DATE_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.START_TIME as START_TIME_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_DAYS as SAVE_DAYS_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.STATUS as STATUS_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SHORT_TITLE as SHORT_TITLE_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.ARCHIVE_DATE_EST as ARCHIVE_DATE_EST_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SOURCE_LOAD_DATE as SOURCE_LOAD_DATE_GOLD,
GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE as OP_TYPE_GOLD,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.MACADDRESS,'*') = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.MAC_ADDRESS,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as MAC_ADDRESS_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.CALLSIGN,'*') = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.CALL_SIGN,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as CALL_SIGN_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.STARTDATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.START_DATE,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as START_DATE_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.STARTTIME,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.START_TIME,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as START_TIME_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.DURATION,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.DURATION,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as DURATION_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.REC_TYPE,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.RECORDING_TYPE,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as RECORDING_TYPE_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_ALL,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_ALL,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as SAVE_ALL_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_LATEST,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_LATEST,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as SAVE_LATEST_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.CREATE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.CREATE_TIME,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as CREATE_TIME_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.MODIFY_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.MODIFIED_TIME,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as MODIFIED_TIME_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.ENDTIME_ADJUSTMENT,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.END_TIME_ADJUSTMENT,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N') as END_TIME_ADJUSTMENT_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_DAYS,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_DAYS,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as SAVE_DAYS_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.STATUS,-1) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.STATUS,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as STATUS_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SHORT_TITLE,'*') = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SHORT_TITLE,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as SHORT_TITLE_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_DATE_EST,cast('${hivevar:default_date}' as date)) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.ARCHIVE_DATE_EST,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as ARCHIVE_DATE_EST_IND,
if(if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SOURCE_LOAD_DATE,cast('${hivevar:default_date}' as date)) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SOURCE_LOAD_DATE,1,0) = 0 OR (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE ='D' AND to_date(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}')),'Y','N' ) as SOURCE_LOAD_DATE_IND,
if(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.HOME_ID is null , 'TRUE','FALSE') as NEWFLAG,
if(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.HOME_ID is not null AND (if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.CALLSIGN,'*') = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.CALL_SIGN,1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.DURATION,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.DURATION,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.ENDTIME_ADJUSTMENT,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.END_TIME_ADJUSTMENT,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.MACADDRESS,'*') = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.MAC_ADDRESS,1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.MODIFY_TIME,to_utc_timestamp('1970-01-01','YYYY-MM-DD HH:MM:SS')) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.MODIFIED_TIME,1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.REC_TYPE,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.RECORDING_TYPE,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_ALL,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_ALL,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_LATEST,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_LATEST,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.STARTDATE,to_utc_timestamp('1970-01-01','YYYY-MM-DD HH:MM:SS')) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.START_DATE,1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.STARTTIME,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.START_TIME,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SAVE_DAYS,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SAVE_DAYS,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.STATUS,-1) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.STATUS,-1),1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SHORT_TITLE,'*') = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SHORT_TITLE,1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.ARCHIVE_DATE_EST,cast('1970-01-01' as date)) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.ARCHIVE_DATE_EST,1,0) = 0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.SOURCE_LOAD_DATE,cast('1970-01-01' as date)) = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SOURCE_LOAD_DATE,1,0) =0 OR if(COALESCE(INCOMING_RSDVR_REQUESTS_ARCHIVE.CREATE_TIME,to_utc_timestamp('1970-01-01','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.CREATE_TIME,to_utc_timestamp('1970-01-01','YYYY-MM-DD HH:MM:SS')),1,0) = 0),'TRUE','FALSE') as CHANGEDFLAG,
cast('${hivevar:dtm_start}' as date) as LOAD_DATE,
current_timestamp as SYSDATE,
'Y' as Y_IND,
'I' as OP_TYPE_I,
IF (GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.OP_TYPE = 'D','I','U') AS OP_TYPE_U
FROM
${hivevar:hive_database_name_incoming}.${hivevar:incoming_rsdvr_requests_archive_tbl} INCOMING_RSDVR_REQUESTS_ARCHIVE LEFT OUTER JOIN ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_requests_archive_last_rec_tbl} GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC
ON
(INCOMING_RSDVR_REQUESTS_ARCHIVE.ID_SCHEDULE = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SCHEDULE_ID AND
INCOMING_RSDVR_REQUESTS_ARCHIVE.ID_HOME = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.HOME_ID AND
INCOMING_RSDVR_REQUESTS_ARCHIVE.SERIALNUM = GOLD_RSDVR_REQUESTS_ARCHIVE_LAST_REC.SERIAL_NUMBER)
WHERE
INCOMING_RSDVR_REQUESTS_ARCHIVE.LOAD_DATE='${hivevar:load_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################