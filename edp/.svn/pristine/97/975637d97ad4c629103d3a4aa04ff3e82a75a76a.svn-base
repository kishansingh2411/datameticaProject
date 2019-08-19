--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_nz_t_f_split_channel_tuning_rst_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
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
    HOUSEHOLD_DEVICE_ID           int           COMMENT '',
	QUARTER_HOUR_OF_DAY_OFFSET	  int           COMMENT '',
	STB_COLLECTION_TASK_ID	      int           COMMENT '',
	CHANNEL_NAME_ID	              int           COMMENT '',
	TMS_NETWORK_ID	              int           COMMENT '',
	TMS_PROGRAM_ID	              int           COMMENT '',
	SEGMENT_START_TIME	          timestamp     COMMENT '',
	SEGMENT_END_TIME	          timestamp     COMMENT '',
	DTM_PROGRAM_START	          timestamp     COMMENT '',
	DTM_PROGRAM_END	              timestamp     COMMENT '',
	SEGMENTED_DURATION	          int           COMMENT '',
	CHANNEL_TUNE_DURATION	      int           COMMENT '',
	STB_FILE_CONTROL_ID	          int           COMMENT '',	
	LOAD_DATE                     timestamp     COMMENT '',
	DTM_CREATED	                  timestamp     COMMENT '',
	DTM_CHANNEL_TUNING_CREATED	  timestamp     COMMENT '',
	DTM_LAST_UPDATED	          timestamp     COMMENT '',
	HOUSEHOLD_ID	              int           COMMENT '',
	CHANNEL_NBR	        	      int           COMMENT '',
	SAMPLE_ID                     int           COMMENT '',
	SEGMENT_DATE	              date          COMMENT '',
	DTM_TUNED_TO_STARTED	      timestamp     COMMENT '',
	DTM_TUNED_TO_ENDED	          timestamp     COMMENT '',
	TUNING_SOURCE_ID	          int           COMMENT '',
	TUNING_TYPE_ID	              int           COMMENT '',
	PLAYBACK_DAY	              int           COMMENT '',
	QTR_SUM_DEV_DURATION	      int           COMMENT '',
	QTR_SUM_DEV_CHN_DUR	          int           COMMENT '',
	QTR_SUM_DEV_CHN_PROG_DUR	  int           COMMENT '',
	BROADCAST_WEEK_ID	          int           COMMENT '',
	BROADCAST_MONTH_ID	          int           COMMENT '',
	CALENDAR_WEEK_ID	          int           COMMENT '',
	CALENDAR_MONTH_ID	          int           COMMENT '',
	DAY_OF_WEEK_ID                int           COMMENT ''
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################