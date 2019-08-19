--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build work_rsdvr_recordings_archive_tmp table 
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
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE TABLE 
   ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_recordings_archive_tmp_tbl} 
SELECT 
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ASSET_ID as ASSET_ID_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_ASSET as ASSET_ID_INCOMING,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.CALL_SIGN as CALL_SIGN_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.CALLSIGN as CALL_SIGN_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.CALLSIGN,'*') = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.CALL_SIGN,'*'),1,0
         ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as CALL_SIGN_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.CREATE_TIME as CREATE_TIME_GOLD, 
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.CREATE_TIME as CREATE_TIME_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.CREATE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.CREATE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as CREATE_TIME_IND,
    GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_STARTTIME_EST as STARTDATE_STARTTIME_EST_GOLD,
    INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_STARTTIME_EST as STARTDATE_STARTTIME_EST_INCOMING,
    if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_STARTTIME_EST,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_STARTTIME_EST,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as STARTDATE_STARTTIME_EST_IND,
    GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ARCHIVE_TIME as ARCHIVE_TIME_GOLD,
    INCOMING_RSDVR_RECORDINGS_ARCHIVE.ARCHIVE_TIME as ARCHIVE_TIME_INCOMING,
    if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ARCHIVE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ARCHIVE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as ARCHIVE_TIME_IND,
   current_timestamp as DTM_SYSDATE,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.DURATION as DURATION_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.DURATION as DURATION_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.DURATION,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.DURATION,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as DURATION_IND,
    GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ENDTIME_ADJUSTMENT_PREV as ENDTIME_ADJUSTMENT_PREV_GOLD,
    INCOMING_RSDVR_RECORDINGS_ARCHIVE.ENDTIME_ADJUSTMENT_PREV as ENDTIME_ADJUSTMENT_PREV_INCOMING,
    if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ENDTIME_ADJUSTMENT_PREV,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ENDTIME_ADJUSTMENT_PREV,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as ENDTIME_ADJUSTMENT_PREV_IND,
    GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ENDTIME_ADJUSTMENT as ENDTIME_ADJUSTMENT_GOLD,
    INCOMING_RSDVR_RECORDINGS_ARCHIVE.ENDTIME_ADJUSTMENT as ENDTIME_ADJUSTMENT_INCOMING,
    if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ENDTIME_ADJUSTMENT,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ENDTIME_ADJUSTMENT,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as ENDTIME_ADJUSTMENT_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.HOME_ID as HOME_ID_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_HOME as HOME_ID_INCOMING,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.KEEP_TYPE_CODE as KEEP_TYPE_CODE_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.KEEP_TYPE_CODE as KEEP_TYPE_CODE_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.KEEP_TYPE_CODE,'*') = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.KEEP_TYPE_CODE,'*'),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as KEEP_TYPE_CODE_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ARCHIVE_DATE_EST as ARCHIVE_DATE_EST_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.ARCHIVE_DATE_EST as ARCHIVE_DATE_EST_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ARCHIVE_DATE_EST,CAST('${hivevar:default_date}' AS DATE)) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ARCHIVE_DATE_EST,CAST('${hivevar:default_date}' AS DATE)),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as ARCHIVE_DATE_EST_IND,
   cast('${hivevar:dtm_start}' as date) as LOAD_DATE,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.SOURCE_LOAD_DATE as SOURCE_LOAD_DATE_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.SOURCE_LOAD_DATE as SOURCE_LOAD_DATE_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.SOURCE_LOAD_DATE,CAST('${hivevar:default_date}' AS DATE)) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.SOURCE_LOAD_DATE,CAST('${hivevar:default_date}' AS DATE)),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as SOURCE_LOAD_DATE_IND,
    GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_EST as STARTDATE_EST_GOLD,
    INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_EST as STARTDATE_EST_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_EST,CAST('${hivevar:default_date}' AS DATE)) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_EST,CAST('${hivevar:default_date}' AS DATE)),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as STARTDATE_EST_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.MODIFY_TIME as MODIFY_TIME_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.MODIFY_TIME as MODIFY_TIME_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.MODIFY_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.MODIFY_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as MODIFY_TIME_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE AS OP_TYPE_GOLD, 
   'I' as OP_TYPE_I,
   IF(
       (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE = 'D'),'I','U'
    ) AS OP_TYPE_U,
    GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.PAUSE_TIME as PAUSE_TIME_GOLD,
    INCOMING_RSDVR_RECORDINGS_ARCHIVE.PAUSE_TIME as PAUSE_TIME_INCOMING,
    if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.PAUSE_TIME,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.PAUSE_TIME,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as PAUSE_TIME_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_DATE as RECORDING_DATE_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_DATE as RECORDING_DATE_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_DATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_DATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as RECORDING_DATE_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_ID as RECORDING_ID_GOLD, 
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_RECORDING as RECORDING_ID_INCOMING, 
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_LOCKED as RECORDING_LOCKED_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_LOCK as RECORDING_LOCKED_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_LOCK,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_LOCKED,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as RECORDING_LOCKED_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_STATUS as RECORDING_STATUS_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_STATUS as RECORDING_STATUS_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_STATUS,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_STATUS,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as RECORDING_STATUS_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_TIME as RECORDING_TIME_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_TIME as RECORDING_TIME_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_TIME,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_TIME,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as RECORDING_TIME_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.SCHEDULE_ID as SCHEDULE_ID_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_SCHEDULE as SCHEDULE_ID_INCOMING,   
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.START_DATE as START_DATE_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE as START_DATE_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.START_DATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as START_DATE_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.START_TIME as START_TIME_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTTIME AS START_TIME_INCOMING,    
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTTIME,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.START_TIME,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as START_TIME_IND,
    GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_STARTTIME as STARTDATE_STARTTIME_GOLD,
    INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_STARTTIME as STARTDATE_STARTTIME_INCOMING,
    if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_STARTTIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_STARTTIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as STARTDATE_STARTTIME_IND,
   GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.TEMPORARY_ASSET_ID as TEMPORARY_ASSET_ID_GOLD,
   INCOMING_RSDVR_RECORDINGS_ARCHIVE.TEMPORARY_ID_ASSET as TEMPORARY_ASSET_ID_INCOMING,
   if(
      if(
         COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.TEMPORARY_ID_ASSET,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.TEMPORARY_ASSET_ID,-1),1,0
        ) = 0 
      OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE ='D'),'Y','N'
    ) as TEMPORARY_ASSET_ID_IND,
   'Y' as Y_IND,
   if(
      GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.SCHEDULE_ID is not null 
      AND GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ASSET_ID is not null 
      AND GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_ID is not null
      AND GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.HOME_ID is not null
      AND (
            (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.CALLSIGN,'*') = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.CALL_SIGN,'*'),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.CREATE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.CREATE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_STARTTIME_EST,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_STARTTIME_EST,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) ,1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ARCHIVE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ARCHIVE_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.DURATION,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.DURATION,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ENDTIME_ADJUSTMENT_PREV,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ENDTIME_ADJUSTMENT_PREV,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ENDTIME_ADJUSTMENT,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ENDTIME_ADJUSTMENT,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.KEEP_TYPE_CODE,'*') = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.KEEP_TYPE_CODE,'*'),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ARCHIVE_DATE_EST,CAST('${hivevar:default_date}' AS DATE)) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ARCHIVE_DATE_EST,CAST('${hivevar:default_date}' AS DATE)),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.SOURCE_LOAD_DATE,CAST('${hivevar:default_date}' AS DATE)) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.SOURCE_LOAD_DATE,CAST('${hivevar:default_date}' AS DATE)),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_EST,CAST('${hivevar:default_date}' AS DATE)) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_EST,CAST('${hivevar:default_date}' AS DATE)),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.MODIFY_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.MODIFY_TIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.PAUSE_TIME,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.PAUSE_TIME,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_DATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_DATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_LOCK,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_LOCKED,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_STATUS,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_STATUS,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.REC_TIME,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_TIME,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.START_DATE,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTDATE_STARTTIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.STARTDATE_STARTTIME,to_utc_timestamp('${hivevar:default_date}','YYYY-MM-DD HH:MM:SS')),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.STARTTIME,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.START_TIME,-1),1,0) = 0)
            OR (if(COALESCE(INCOMING_RSDVR_RECORDINGS_ARCHIVE.TEMPORARY_ID_ASSET,-1) = COALESCE(GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.TEMPORARY_ASSET_ID,-1),1,0) = 0)
            OR (GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.OP_TYPE = 'D' AND to_date(INCOMING_RSDVR_RECORDINGS_ARCHIVE.ARCHIVE_TIME) = to_date('${hivevar:dtm_end}'))
      ),'TRUE','FALSE') as CHANGEDFLAG,
   if(
      GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.SCHEDULE_ID is null 
      AND GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ASSET_ID is null
      AND GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.HOME_ID is null
      AND GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_ID is null
      , 'TRUE','FALSE'
    ) as NEWFLAG
FROM
   ${hivevar:hive_database_name_incoming}.${hivevar:incoming_rsdvr_recordings_archive_tbl} INCOMING_RSDVR_RECORDINGS_ARCHIVE 
LEFT OUTER JOIN ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_recordings_archive_last_rec_tbl} GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC
   ON (
       INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_SCHEDULE = GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.SCHEDULE_ID 
       AND INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_ASSET = GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.ASSET_ID
       AND INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_HOME = GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.HOME_ID
       AND INCOMING_RSDVR_RECORDINGS_ARCHIVE.ID_RECORDING = GOLD_RSDVR_RECORDINGS_ARCHIVE_LAST_REC.RECORDING_ID
      )
WHERE 
	INCOMING_RSDVR_RECORDINGS_ARCHIVE.load_date='${hivevar:load_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################