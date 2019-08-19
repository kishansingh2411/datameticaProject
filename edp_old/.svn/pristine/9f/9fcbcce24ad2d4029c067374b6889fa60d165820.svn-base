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

INSERT OVERWRITE 
   TABLE ${hivevar:target_hive_database}.${hivevar:target_table}
   SELECT
	 stb_collection_task_id ,
	 collection_task_identification ,
	 dtm_to_start ,
	 dtm_to_finish ,
	 dtm_task_published ,
	 dtm_task_suspended ,
	 dtm_created ,
	 dtm_last_updated ,
	 sourced_from_system ,
	 last_updated_by_system ,
	 aggregator_string ,
	 pre_condition_string ,
	 param_string ,
	 sample_descr ,
	 sample_id ,
	 tgt_dtm_created ,
	 tgt_dtm_last_updated ,
	 tgt_sample ,
	 fact_load_flag
FROM 
	 ${hivevar:src_hive_database}.${hivevar:src_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################