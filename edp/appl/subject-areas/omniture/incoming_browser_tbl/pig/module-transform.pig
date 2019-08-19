--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming Browser table from work.
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

incoming_browser = 
   LOAD '${target_database}.${target_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_incoming_browser =
   FILTER incoming_browser BY suite_id == '$suite_id'
;

work_browser = 
   LOAD '${source_database}.${source_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_work_browser =
   FILTER work_browser BY (suite_id == '$suite_id') AND (source_date == '$source_date')
;

incoming_join_work_browser =
   JOIN filter_work_browser BY browser_id left,
   filter_incoming_browser BY browser_id
   USING 'replicated'
;

incoming_minus_work_browser =
   FILTER incoming_join_work_browser BY (filter_incoming_browser::browser_id is null)
;

incoming_out_work_browser = FOREACH incoming_minus_work_browser
   GENERATE filter_work_browser::suite_id as suite_id,
   filter_work_browser::browser_id as browser_id,
   filter_work_browser::browser_name as browser_name   
;

incoming_final_output = FOREACH incoming_out_work_browser
   GENERATE browser_id as browser_id,
   browser_name as browser_name,
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