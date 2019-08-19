--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Incoming table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/channel_tuning/
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/
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

INSERT INTO TABLE ${hivevar:target_hive_database}.${hivevar:target_table}
     PARTITION(segment_date)
SELECT
	 household_device_id,
	 quarter_hour_of_day_offset,
	 stb_collection_task_id,
	 channel_name_id,
	 tms_network_id,
	 tms_program_id,
	 segment_start_time,
	 segment_end_time,
	 dtm_program_start,
	 dtm_program_end,
	 segmented_duration,
	 channel_tune_duration,
	 stb_file_control_id,
	 load_date,
	 dtm_created,
	 dtm_channel_tuning_created,
	 dtm_last_updated,
	 household_id,
	 channel_nbr,
	 sample_id,
	 dtm_tuned_to_started,
	 dtm_tuned_to_ended,
	 tuning_source_id,
	 tuning_type_id,
	 playback_day,
	 qtr_sum_dev_duration,
	 qtr_sum_dev_chn_dur,
	 qtr_sum_dev_chn_prog_dur,
	 broadcast_week_id,
	 broadcast_month_id,
	 calendar_week_id,
	 calendar_month_id,
	 day_of_week_id,
	 chn_prog_consec_dur,
	 broadcast_month,
	 segment_date
FROM 
	 ${hivevar:src_hive_database}.${hivevar:src_table}
WHERE 
	 broadcast_month='${hivevar:filter_column}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################