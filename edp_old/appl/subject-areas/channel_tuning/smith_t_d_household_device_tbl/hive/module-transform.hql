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
	 household_device_id ,
	 household_key1 ,
	 device_key1 ,
	 device_key2 ,
	 device_make ,
	 device_model ,
	 zip_code ,
	 sourced_from_system ,
	 last_updated_by_system ,
	 dtm_created ,
	 dtm_last_updated ,
	 ins_stb_file_control_id ,
	 upd_stb_file_control_id ,
	 created_by ,
	 household_type ,
	 household_type_descr ,
	 household_state ,
	 household_state_descr ,
	 last_updated_by ,
	 dtm_of_disconnect ,
	 disconnect_code ,
	 disconnect_descr ,
	 device_state ,
	 dtm_provisioned ,
	 household_key2 ,
	 household_key3 ,
	 household_key4 ,
	 household_key5 ,
	 corp ,
	 corp_franchise_tax_area ,
	 non_disclosure_ind ,
	 dtm_of_non_disclosure_request ,
	 household_id ,
	 corp_name ,
	 corp_franchise_tax_area_name ,
	 household_key6 ,
	 zip_code6 ,
	 census ,
	 mdu ,
	 dtm_original_install ,
	 op_type ,
	 tgt_dtm_created ,
	 tgt_dtm_last_updated ,
	 device_ext_key1 ,
	 device_ext_key2 ,
	 household_ext_key1 ,
	 household_ext_key2
FROM 
	 ${hivevar:src_hive_database}.${hivevar:src_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################