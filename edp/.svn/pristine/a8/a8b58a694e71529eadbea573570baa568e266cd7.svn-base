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
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 05/02/2016
--#   Log File    : 
--#   SQL File    : 
--#   Error File  : 
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/02/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.compress.output=true;
set avro.output.codec=snappy;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} 
   PARTITION (Broadcast_Month_Id,Broadcast_week_id)
SELECT 
    HOUSEHOLD_DEVICE_ID        ,
	QUARTER_HOUR_OF_DAY_OFFSET ,
	STB_COLLECTION_TASK_ID	   ,
	CHANNEL_NAME_ID	           ,
	TMS_NETWORK_ID	           ,
	TMS_PROGRAM_ID	           ,
	SEGMENT_START_TIME	       ,
	SEGMENT_END_TIME	       ,
	DTM_PROGRAM_START	       ,
	DTM_PROGRAM_END	           ,
	SEGMENTED_DURATION	       ,
	CHANNEL_TUNE_DURATION	   ,
	STB_FILE_CONTROL_ID	       ,	
	LOAD_DATE                  ,
	DTM_CREATED	               ,
	DTM_CHANNEL_TUNING_CREATED ,
	DTM_LAST_UPDATED	       ,
	HOUSEHOLD_ID	           ,
	CHANNEL_NBR	        	   ,
	SAMPLE_ID                  ,
	SEGMENT_DATE	           ,
	DTM_TUNED_TO_STARTED	   ,
	DTM_TUNED_TO_ENDED	       ,
	TUNING_SOURCE_ID	       ,
	TUNING_TYPE_ID	           ,
	PLAYBACK_DAY	           ,
	QTR_SUM_DEV_DURATION	   ,
	QTR_SUM_DEV_CHN_DUR	       ,
	QTR_SUM_DEV_CHN_PROG_DUR   ,
	CALENDAR_WEEK_ID	       ,
	CALENDAR_MONTH_ID	       ,
	DAY_OF_WEEK_ID             ,
	BROADCAST_MONTH_ID         ,   
	BROADCAST_WEEK_ID          
FROM ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################