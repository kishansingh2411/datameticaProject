--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from work_rsdvr_requests_tbl to incoming_rsdvr_requests_tbl table
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
register ${cvs_jar_path}/CVSecurityApplication-jar-with-dependencies.jar;

set mapred.output.compress true;
set mapred.output.compression.type BLOCK;
set avro.output.codec deflate;

define encryptDecrypt com.cablevision.edh.udf.EncryptDecryptUtil();

work_data = 
	LOAD '${hive_database_name_work}.${work_data_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

--##############################################################################
--#                                 Encrypt Data                               #
--##############################################################################	

work_data_encrypted_records = FOREACH work_data 
        GENERATE id_schedule, 
        encryptDecrypt('${dataset}', 'HOME_ID','${username}',id_home, 'ENCRYPT','${namenode_service}') as id_home,
        encryptDecrypt('${dataset}', 'SERIAL_NUMBER','${username}',serialnum, 'ENCRYPT','${namenode_service}') as serialnum,
		encryptDecrypt('${dataset}', 'MAC_ADDRESS','${username}',macaddress, 'ENCRYPT','${namenode_service}') as macaddress,
		callsign..
;
	
--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE work_data_encrypted_records
   INTO '${hive_database_name_incoming}.${incoming_data_tbl}' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('load_date=$load_date')
; 
--##############################################################################
--#                              End                                           #
--##############################################################################			
