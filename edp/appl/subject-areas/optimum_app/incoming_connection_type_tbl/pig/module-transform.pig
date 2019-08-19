--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming Connection_type table from work.
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

incoming_connection_type = 
   LOAD '${target_database}.${target_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_incoming_connection_type =
   FILTER incoming_connection_type BY suite_id == '$suite_id'
;

work_connection_type = 
   LOAD '${source_database}.${source_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_work_connection_type =
   FILTER work_connection_type BY (suite_id == '$suite_id') AND (source_date == '$source_date')
;

incoming_join_work_connection_type =
   JOIN filter_work_connection_type BY connection_type_id LEFT,
   filter_incoming_connection_type BY connection_type_id
   USING 'replicated'
;

incoming_minus_work_connection_type =
   FILTER incoming_join_work_connection_type BY (filter_incoming_connection_type::connection_type_id is null)
;

incoming_out_work_connection_type = FOREACH incoming_minus_work_connection_type
   GENERATE filter_work_connection_type::suite_id as suite_id,
   filter_work_connection_type::connection_type_id as connection_type_id,
   filter_work_connection_type::connection_type as connection_type   
;


incoming_final_output = FOREACH incoming_out_work_connection_type
   GENERATE connection_type_id as connection_type_id,
   connection_type as connection_type,
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