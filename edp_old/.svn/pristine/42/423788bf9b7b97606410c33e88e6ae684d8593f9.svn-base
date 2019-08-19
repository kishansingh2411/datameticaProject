--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from landing directory to work table
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog remove_carriage.pig
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
--#                                   Load                                     #
--##############################################################################

DEFINE hdfs_script_path `remove_carriage.ksh` cache('$hdfs_script_path#remove_carriage.ksh');

hit_data_file = LOAD '${source_file_path}' USING PigStorage('\t');

filter_hit_data = STREAM hit_data_file THROUGH hdfs_script_path;

STORE filter_hit_data INTO '${destination_file_path}' USING PigStorage('\t');