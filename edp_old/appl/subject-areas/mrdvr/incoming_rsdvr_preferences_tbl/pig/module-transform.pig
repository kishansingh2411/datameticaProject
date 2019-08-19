--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from work_rsdvr_preferences_tbl to incoming_rsdvr_preferences_tbl table
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

set mapred.output.compress true;
set mapred.output.compression.type BLOCK;
set avro.output.codec deflate;

work_data = 
	LOAD '${hive_database_name_work}.${work_data_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

work_data_distinct = 
     DISTINCT work_data;

	
--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE work_data_distinct
    INTO '${hive_database_name_incoming}.${incoming_data_tbl}' 
    USING org.apache.hive.hcatalog.pig.HCatStorer('load_date=$load_date')
; 
--##############################################################################
--#                              End                                           #
--##############################################################################			
