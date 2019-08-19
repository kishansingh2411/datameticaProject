--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from incoming_hit_data_stock36_raw_tbl to incoming_hit_data_stock36_tbl
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

define channel900Decryptor com.cablevision.edh.udf.Channel900Decryptor();
define encryptDecrypt com.cablevision.edh.udf.EncryptDecryptUtil();

incoming_hit_data_stock36_raw = 
	LOAD '${hive_database_name_incoming}.${incoming_hit_data_stock36_raw_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

--##############################################################################
--#                                 Decrypt                                    #
--##############################################################################

hit_data_raw_decrypted_records = 
	FOREACH incoming_hit_data_stock36_raw 
	GENERATE ..prop1,
	channel900Decryptor(prop1) as mac_id_dyc,
	prop2 .. prop10,
	channel900Decryptor(prop10) as home_id_dyc,
	prop11..
;

--##############################################################################
--#                                 Encrypt with EDP key                       #
--##############################################################################

hit_data_raw_encrypted_records =
	FOREACH hit_data_raw_decrypted_records 
	GENERATE ..prop1,
	encryptDecrypt('$suite_name', 'MAC_ID','$username',mac_id_dyc, 'ENCRYPT', '$namenode_service') as mac_id,
	prop2 .. prop10,
	encryptDecrypt('$suite_name', 'HOME_ID','$username',home_id_dyc, 'ENCRYPT','$namenode_service') as home_id,
	prop11..
;

--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE hit_data_raw_encrypted_records
   INTO '${hive_database_name_incoming}.${incoming_hit_data_stock36_tbl}' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_name=$suite_name, source_date=$source_date')
; 
	
--##############################################################################
--#                              End                                           #
--##############################################################################			