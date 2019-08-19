--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming derived lookup tables from Incoming_Hit_data table.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
--#   Date        : 12/28/2015
--#   Log File    : .../log/${suite_name}/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${suite_name}/${job_name}.log
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

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

import '${pig_script_path}';

-- Loading tables

incoming_optimum_error_msg = 
   LOAD '${source_database}.${error_message_table}'
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

-- Filtering tables

suite_incoming_optimum_error_msg =
   FILTER incoming_optimum_error_msg BY (suite_id == '${suite_id}')
;

--# Deriving distinct ids for lookup table

not_null_distinct_error_msg_desc_hit_data =
   derive_lookup (curr_incoming_hit_data, 'prop35', 'opt_err_msg_desc')
;

--# Joining with distinct datasets

incoming_optimum_error_msg_desc_hit_data_join =
   JOIN not_null_distinct_error_msg_desc_hit_data BY opt_err_msg_desc left,
   suite_incoming_optimum_error_msg BY opt_err_msg_desc
   USING 'replicated'
;

--# Deriving new values from joined datasets

incoming_optimum_new_error_msg_desc =
   FILTER incoming_optimum_error_msg_desc_hit_data_join BY 
   (suite_incoming_optimum_error_msg::opt_err_msg_desc is null)
;

--# Adding ids to newly derived datasets

rank_incoming_optimum_new_error_msg_desc = RANK incoming_optimum_new_error_msg_desc 
   BY not_null_distinct_error_msg_desc_hit_data::opt_err_msg_desc asc
;

--# Deriving unique ids for newly derived lookup values

incoming_optimum_error_msg_out = 
   FOREACH rank_incoming_optimum_new_error_msg_desc GENERATE 
   (bigdecimal)rank_incoming_optimum_new_error_msg_desc + ${max_error_message_id} as opt_err_msg_id,
   not_null_distinct_error_msg_desc_hit_data::opt_err_msg_desc as opt_err_msg_desc,
   'omniture' as sourced_from_system,
   CurrentTime() as dtm_created   
;

--# Storing new datasets in tables

STORE incoming_optimum_error_msg_out 
   INTO '${source_database}.${error_message_table}'
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_id=${suite_id},source_date=${source_date}')
;

--##############################################################################
--#                                    End                                     #
--##############################################################################