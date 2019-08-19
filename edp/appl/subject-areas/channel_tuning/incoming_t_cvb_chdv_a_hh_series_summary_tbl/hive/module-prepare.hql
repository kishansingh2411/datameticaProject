--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_t_cvb_chdv_a_hh_series_summary_hst_tbl)
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
   PARTITION (YEAR_WEEK)
SELECT 
  HOUSEHOLD_ID  ,
  PLAYBACK_VIEWING_AGGR_ID, 
  CUMULATIVE_PLAYBACK_DAYS,
   TUNING_TYPE , 
   TUNING_SOURCE   , 
   CHANNEL_NAME_ID  ,
   TMS_PROGRAM_ID  ,
   SAMPLE_ID , 
   TITLE ,
   VIEWING_DRTN ,           
   VIEWING_CNT,
   DEVICE_COUNT ,
   DTM_CREATED ,
   DTM_LAST_UPDATED ,
   DTM_MODIFIED   , 
   SOURCED_FROM_SYSTEM,
   LAST_UPDATED_BY_SYSTEM  ,
   YEAR_WEEK
FROM ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################