--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from work_rsdvr_recordings_archive_tmp table to work_rsdvr_recordings_archive_seq table
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

register ${udf_jar_path}/pig-udf-bank-${version}.jar;
register ${postgres_jar_path};
define seqGen com.cablevision.edh.udf.SequenceGenerator();

--##############################################################################
--#                                 Load Data                                  #
--##############################################################################

work_tmp_data = 
	LOAD '${hive_database_name_work}.${work_rsdvr_recordings_archive_tmp_tbl}'
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

--##############################################################################
--#                                 Filter                                     #
--##############################################################################

work_tmp_data_filter = 
	FILTER work_tmp_data 
	BY (newflag=='TRUE' OR changedflag=='TRUE' )
;

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

generated_records = 
	FOREACH work_tmp_data_filter 
	GENERATE ..recording_time_ind,
		seqGen( '${table_name}' , '${namenode_service}') as rsdvr_recordings_delta_seq,
		schedule_id_gold..
;

--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE generated_records
	INTO '${hive_database_name_work}.${work_rsdvr_recordings_archive_seq_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatStorer()
; 

--##############################################################################
--#                              End                                           #
--##############################################################################			