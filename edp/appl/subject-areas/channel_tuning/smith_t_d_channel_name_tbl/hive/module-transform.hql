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
     channel_name_id ,            
	 channel_name,             
	 sourced_from_system,
	 last_updated_by_system,
	 dtm_created,
	 dtm_last_updated,
	 ins_stb_file_control_id,
	 upd_stb_file_control_id,
	 tms_network_id,
	 tms_network_nbr,
	 tms_network_time_zone,
	 tms_network_name,
	 tms_fcc_call_sign,
	 tms_network_affiliate,
	 tms_network_city,
	 tms_network_state,
	 tms_network_zip_code,
	 tms_network_country,
	 tms_designates_market_name,
	 tms_designates_market_nbr,
	 tms_fcc_channel_nbr,
	 tms_network_language,
	 channel_descr,
	 cvc_call_sign,
	 peg_ind,
	 channel_category,
	 woodbury_lineup_channel_nbr,
	 parent_network,
	 network_group,
	 network_alias,
	 program_exclusion_ind,       
	 insertable_ind,
	 broadcast_flag,
	 strata_name,
	 tgt_dtm_created ,
	 tgt_dtm_last_updated,
	 strata_station_id
FROM 
	 ${hivevar:src_hive_database}.${hivevar:src_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################