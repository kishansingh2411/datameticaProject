--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming Plugins table from work.
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

--# Combining two datasets into one table by
--#performing union on both datasets.

incoming_plugins = 
   LOAD '${target_database}.${target_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_incoming_plugins =
   FILTER incoming_plugins BY suite_id == '$suite_id'
;

work_plugins = 
   LOAD '${source_database}.${source_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_work_plugins =
   FILTER work_plugins BY (suite_id == '$suite_id') AND (source_date == '$source_date')
;

incoming_join_work_plugins =
   JOIN filter_work_plugins BY plugin_id LEFT,
   filter_incoming_plugins BY plugin_id
   USING 'replicated'
;

incoming_minus_work_plugins =
   FILTER incoming_join_work_plugins BY (filter_incoming_plugins::plugin_id is null)
;

incoming_out_work_plugins = FOREACH incoming_minus_work_plugins
   GENERATE filter_work_plugins::suite_id as suite_id,
   filter_work_plugins::plugin_id as plugin_id,
   filter_work_plugins::plugin_name as plugin_name   
;

incoming_final_output = FOREACH incoming_out_work_plugins
   GENERATE plugin_id as plugin_id,
   plugin_name as plugin_name,
   CurrentTime() as dtm_created,
   'omniture' as sourced_from_system
;

STORE incoming_final_output
   INTO '${target_database}.${target_table}' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_id=$suite_id,source_date=$source_date')
;

--##############################################################################
--#                                    End                                     #
--##############################################################################