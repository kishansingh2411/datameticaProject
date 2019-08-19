--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming Javascript_version table from work.
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

incoming_javascript_version = 
   LOAD '${target_database}.${target_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_incoming_javascript_version =
   FILTER incoming_javascript_version BY suite_id == '$suite_id'
;

work_javascript_version = 
   LOAD '${source_database}.${source_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_work_javascript_version =
   FILTER work_javascript_version BY (suite_id == '$suite_id') AND (source_date == '$source_date')
;

incoming_join_work_javascript_version =
   JOIN filter_work_javascript_version  BY javascript_version_id LEFT,
   filter_incoming_javascript_version BY javascript_version_id
   USING 'replicated'
;

incoming_minus_work_javascript_version =
   FILTER incoming_join_work_javascript_version BY 
   (filter_incoming_javascript_version::javascript_version_id is null)
;

incoming_out_work_javascript_version = 
   FOREACH incoming_minus_work_javascript_version
   GENERATE filter_work_javascript_version::suite_id as suite_id,
   filter_work_javascript_version::javascript_version_id as javascript_version_id,
   filter_work_javascript_version::javascript_version as javascript_version   
;

incoming_final_output = FOREACH incoming_out_work_javascript_version
   GENERATE javascript_version_id as javascript_version_id,
   javascript_version as javascript_version,
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