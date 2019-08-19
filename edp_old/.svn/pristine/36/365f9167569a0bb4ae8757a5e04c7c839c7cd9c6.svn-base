--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from incoming_hit_data table to gold_uni_hit_data table
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
--#                                   Load                                     #
--##############################################################################

define derive_lookup (curr_incoming_hit_data, source_column, lookup_column)
returns not_null_distinct_source_col_hit_data {

   extract_source_column_hit_data = FOREACH $curr_incoming_hit_data
      GENERATE (TRIM($source_column) == '' ? null : $source_column) as $lookup_column
;
 
   distinct_source_column_hit_data =
      DISTINCT extract_source_column_hit_data
;

   $not_null_distinct_source_col_hit_data =
      FILTER distinct_source_column_hit_data BY $lookup_column is not null
;

};

--##############################################################################
--#                              End                                           #
--##############################################################################