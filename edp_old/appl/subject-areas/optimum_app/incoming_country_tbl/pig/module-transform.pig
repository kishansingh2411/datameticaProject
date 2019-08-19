--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming Country table from work.
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

--# Combining two datasets into one table by
--#performing union on both datasets.

incoming_country = 
   LOAD '${target_database}.${target_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_incoming_country =
   FILTER incoming_country BY suite_id == '$suite_id'
;

work_country = 
   LOAD '${source_database}.${source_table}' 
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

filter_work_country =
   FILTER work_country BY (suite_id == '$suite_id') AND (source_date == '$source_date')
;

incoming_join_work_country =
   JOIN filter_work_country BY country_id LEFT,
   filter_incoming_country BY country_id
   USING 'replicated'
;

incoming_minus_work_country =
   FILTER incoming_join_work_country BY (filter_incoming_country::country_id is null)
;

incoming_out_work_country = FOREACH incoming_minus_work_country
   GENERATE filter_work_country::suite_id as suite_id,
   filter_work_country::country_id as country_id,
   filter_work_country::country_name as country_name   
;

incoming_final_output = FOREACH incoming_out_work_country
   GENERATE country_id as country_id,
   country_name as country_name,
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