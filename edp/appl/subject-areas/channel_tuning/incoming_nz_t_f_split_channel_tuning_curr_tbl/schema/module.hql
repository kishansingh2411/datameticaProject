--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_nz_t_f_split_channel_tuning_6mth_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : ...log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/30/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(   
   HOUSEHOLD_DEVICE_ID          INT     		COMMENT '',
   QUARTER_HOUR_OF_DAY_OFFSET   INT     		COMMENT '',
   STB_COLLECTION_TASK_ID       INT     		COMMENT '',
   CHANNEL_NAME_ID              INT     		COMMENT '',
   TMS_NETWORK_ID               INT     		COMMENT '',
   TMS_PROGRAM_ID               INT     		COMMENT '',
   SEGMENT_START_TIME           TIMESTAMP     	COMMENT '',
   SEGMENT_END_TIME             TIMESTAMP     	COMMENT '',
   DTM_PROGRAM_START            TIMESTAMP     	COMMENT '',
   DTM_PROGRAM_END              TIMESTAMP     	COMMENT '',
   SEGMENTED_DURATION           INT     		COMMENT '',
   CHANNEL_TUNE_DURATION        INT     		COMMENT '',
   STB_FILE_CONTROL_ID          INT     		COMMENT '',
   LOAD_DATE                    TIMESTAMP     	COMMENT '',
   DTM_CREATED                  TIMESTAMP     	COMMENT '',
   DTM_CHANNEL_TUNING_CREATED   TIMESTAMP     	COMMENT '',
   DTM_LAST_UPDATED             TIMESTAMP     	COMMENT '',
   HOUSEHOLD_ID                 INT    			COMMENT '',
   CHANNEL_NBR                  INT     		COMMENT '',
   SAMPLE_ID                    INT     		COMMENT '',
   SEGMENT_DATE                 DATE         	COMMENT '',
   DTM_TUNED_TO_STARTED         TIMESTAMP     	COMMENT '',
   DTM_TUNED_TO_ENDED           TIMESTAMP     	COMMENT '',
   TUNING_SOURCE_ID             INT     		COMMENT '',
   TUNING_TYPE_ID               INT     		COMMENT '',
   PLAYBACK_DAY                 INT     		COMMENT '',
   QTR_SUM_DEV_DURATION         INT     		COMMENT '',
   QTR_SUM_DEV_CHN_DUR          INT     		COMMENT '',
   QTR_SUM_DEV_CHN_PROG_DUR     INT     		COMMENT '',
   BROADCAST_WEEK_ID            INT     		COMMENT '',
   BROADCAST_MONTH_ID           INT     		COMMENT '',
   CALENDAR_WEEK_ID             INT     		COMMENT '',
   CALENDAR_MONTH_ID            INT     		COMMENT '',
   DAY_OF_WEEK_ID               INT     		COMMENT '',
   CHN_PROG_CONSEC_DUR          INT     		COMMENT ''
)
PARTITIONED BY (BROADCAST_MONTH STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '^'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################