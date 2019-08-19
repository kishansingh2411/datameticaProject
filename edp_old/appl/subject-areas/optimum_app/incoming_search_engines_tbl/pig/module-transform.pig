--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming Search_engines table from work.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
--#   Date        : 09/12/2016
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
--#    1.0     DataMetica Team          09/12/2016       Initial version
--#
--#
--#####################################################################################################################

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

--# Combining two datasets into one table by
--#performing union on both datasets.

incoming_search_engines = 
   LOAD '${target_database}.${target_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_incoming_search_engines =
   FILTER incoming_search_engines BY suite_id == '$suite_id'
;

work_search_engines = 
   LOAD '${source_database}.${source_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_work_search_engines =
   FILTER work_search_engines BY (suite_id == '$suite_id') AND (source_date == '$source_date')
;

incoming_join_work_search_engines =
   JOIN filter_work_search_engines BY search_engine_id LEFT,
   filter_incoming_search_engines BY search_engine_id
   USING 'replicated'
;

incoming_minus_work_search_engines =
   FILTER incoming_join_work_search_engines BY (filter_incoming_search_engines::search_engine_id is null)
;

incoming_out_work_search_engines = FOREACH incoming_minus_work_search_engines
   GENERATE filter_work_search_engines::suite_id as suite_id,
   filter_work_search_engines::search_engine_id as search_engine_id,
   filter_work_search_engines::search_engine_name as search_engine_name   
;

incoming_final_output = FOREACH incoming_out_work_search_engines
   GENERATE search_engine_id as search_engine_id,
   search_engine_name as search_engine_name,
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