--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(gold_rsdvr_requests_tbl)
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

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   SCHEDULE_ID                   BIGINT               COMMENT '',
   HOME_ID                       VARCHAR(32)          COMMENT 'home id of the requesting box',
   SERIAL_NUMBER                 VARCHAR(32)          COMMENT 'serial number of the requesting box',
   MAC_ADDRESS                   VARCHAR(32)          COMMENT 'macaddress of the requesting box',
   CALL_SIGN                     VARCHAR(12)          COMMENT 'callsign of the channel being recorded',
   START_DATE                    TIMESTAMP            COMMENT 'date recording is to start',
   START_TIME                    VARCHAR(8)           COMMENT 'time recording is to start',
   DURATION                      INT                  COMMENT 'duration of the recording in minutes',
   RECORDING_TYPE                INT                  COMMENT 'recording type - see lookup table for values',
   SAVE_ALL                      INT                  COMMENT 'Save all recordings associated with this request - see lookup table for values',
   SAVE_LATEST                   INT                  COMMENT 'number of latest recordings to save - see lookup table for values',
   CREATE_TIME                   TIMESTAMP            COMMENT 'date/time request created',
   MODIFIED_TIME                 TIMESTAMP            COMMENT 'date/time request modified',
   END_TIME_ADJUSTMENT           INT                  COMMENT '',
   SAVE_DAYS                     DECIMAL(5, 0)        COMMENT '',
   STATUS                        BIGINT               COMMENT '',
   SHORT_TITLE                   VARCHAR(128)         COMMENT '',
   DS_STARTTIME                  TIMESTAMP            COMMENT '',
   LAST_UPDATED                  TIMESTAMP            COMMENT '',
   MAC_ADDRESS_IND               CHAR(1)           COMMENT '',
   CALL_SIGN_IND                 CHAR(1)           COMMENT '',
   START_DATE_IND                CHAR(1)           COMMENT '',
   START_TIME_IND                CHAR(1)           COMMENT '',
   DURATION_IND                  CHAR(1)           COMMENT '',
   RECORDING_TYPE_IND            CHAR(1)           COMMENT '',
   SAVE_ALL_IND                  CHAR(1)           COMMENT '',
   SAVE_LATEST_IND               CHAR(1)           COMMENT '',
   MODIFIED_TIME_IND             CHAR(1)           COMMENT '',
   END_TIME_ADJUSTMENT_IND       CHAR(1)           COMMENT '',
   SAVE_DAYS_IND                 CHAR(1)    COMMENT'',
   STATUS_IND                    CHAR(1)    COMMENT'',
   SHORT_TITLE_IND               CHAR(1)    COMMENT'',
   DS_STARTTIME_IND              CHAR(1)    COMMENT'',
   LAST_UPDATED_IND              CHAR(1)    COMMENT'',
   CREATE_TIME_IND               CHAR(1)    COMMENT'',
   OP_TYPE                       CHAR(1)           COMMENT '',
   DTM_CREATED                   TIMESTAMP            COMMENT '',
   RSDVR_REQUESTS_DELTA_SEQ      BIGINT               COMMENT ''
)
PARTITIONED BY (LOAD_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;

--##############################################################################
--#                                    End                                     #
--##############################################################################