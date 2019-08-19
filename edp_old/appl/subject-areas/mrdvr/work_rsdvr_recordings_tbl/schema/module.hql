--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(work_rsdvr_recordings_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 05/05/2016
--#   Log File    : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/05/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(    
   CALLSIGN                VARCHAR(12) COMMENT 'callsign of the channel being recorded',
   CREATE_TIME             TIMESTAMP   COMMENT 'date/time recording was created',
   DS_RECTIME              TIMESTAMP   COMMENT 'rectime in Eastern Time',
   DS_STARTTIME            TIMESTAMP   COMMENT 'start_time in Eastern Time',
   DURATION                BIGINT      COMMENT  'duration of the recording in minutes',
   ENDTIME_ADJUSTMENT_PREV BIGINT      COMMENT  '',
   ENDTIME_ADUSTMENT       BIGINT      COMMENT  '',
   ID_ASSET                BIGINT      COMMENT 'asset id as generated by the asset management system',
   ID_HOME                 BIGINT      COMMENT  '',
   ID_SCHEDULE             BIGINT      COMMENT  'links to the RSDVR_REQUESTS table',
   ID_RECORDING            BIGINT      COMMENT  'unique id of the recording',
   KEEP_TYPE_CODE          VARCHAR(4)  COMMENT  '',
   LAST_UPDATED            TIMESTAMP   COMMENT  '',
   LOADTIME                TIMESTAMP   COMMENT 'date and time data loaded into the DB',
   MODIFY_TIME             TIMESTAMP   COMMENT 'date/time the recording was modified',
   PAUSE_TIME              TIMESTAMP   COMMENT 'the time of the pause that initiated this temporary recording. this will only be populated for recordings that were saved after a pause action',
   REC_DATE                TIMESTAMP   COMMENT 'date of the recording',
   REC_LOCK                BIGINT      COMMENT  'specified whether the recording is locked (parental controls)',
   REC_STATUS              BIGINT      COMMENT  'status of the recording - see lookup table for values',
   REC_TIME                VARCHAR(8)  COMMENT  'Start time of the recording',
   STARTDATE               TIMESTAMP   COMMENT 'date recording is to start',
   STARTDATE_STARTTIME     TIMESTAMP   COMMENT 'start date time of the recording',
   STARTTIME               VARCHAR(8)  COMMENT  'time recording is to start',
   TEMPORARY_ID_ASSET      BIGINT      COMMENT 'temporary asset id that will be used to turn this recording into a permanent one. this will only be populated for recordings that were saved after a pause action'
)
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################