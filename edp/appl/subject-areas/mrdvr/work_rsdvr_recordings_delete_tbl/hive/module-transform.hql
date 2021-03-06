--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build work_rsdvr_recordings_tmp table 
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

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE 
   TABLE ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_recordings_delete_tbl}
SELECT 
   WORK_RSDVR_RECORDINGS_TMP.HOME_ID_INCOMING                AS HOME_ID,
   WORK_RSDVR_RECORDINGS_TMP.ASSET_ID_INCOMING               AS ASSET_ID,
   WORK_RSDVR_RECORDINGS_TMP.SCHEDULE_ID_INCOMING            AS SCHEDULE_ID,
   WORK_RSDVR_RECORDINGS_TMP.RECORDING_ID_INCOMING           AS RECORDING_ID,
   WORK_RSDVR_RECORDINGS_TMP.CREATE_TIME_INCOMING            AS CREATE_TIME,
   'D'                                                       AS OP_TYPE,
   GOLD_RSDVR_RECORDINGS_LAST_REC.DTM_CREATED                AS DTM_CREATED,
   GOLD_RSDVR_RECORDINGS_LAST_REC.RSDVR_RECORDINGS_DELTA_SEQ AS RSDVR_RECORDINGS_DELTA_SEQ
FROM 
   ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_recordings_tmp_tbl} WORK_RSDVR_RECORDINGS_TMP 
   LEFT JOIN ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_recordings_last_rec_tbl} GOLD_RSDVR_RECORDINGS_LAST_REC   
   ON
      (
         GOLD_RSDVR_RECORDINGS_LAST_REC.HOME_ID = WORK_RSDVR_RECORDINGS_TMP.HOME_ID_INCOMING 
         AND GOLD_RSDVR_RECORDINGS_LAST_REC.ASSET_ID = WORK_RSDVR_RECORDINGS_TMP.ASSET_ID_INCOMING 
         AND GOLD_RSDVR_RECORDINGS_LAST_REC.SCHEDULE_ID = WORK_RSDVR_RECORDINGS_TMP.SCHEDULE_ID_INCOMING 
         AND GOLD_RSDVR_RECORDINGS_LAST_REC.RECORDING_ID = WORK_RSDVR_RECORDINGS_TMP.RECORDING_ID_INCOMING
     )
WHERE
   GOLD_RSDVR_RECORDINGS_LAST_REC.OP_TYPE !='D'
   AND to_date(WORK_RSDVR_RECORDINGS_TMP.LOADTIME_INCOMING) < to_date('${hivevar:dtm_start}')
;

--##############################################################################
--#                                    End                                     #
--##############################################################################