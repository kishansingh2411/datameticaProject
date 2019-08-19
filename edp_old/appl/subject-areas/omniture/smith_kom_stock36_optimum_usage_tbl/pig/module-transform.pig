--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from gold_hit_data_stock36_tbl to smith_kom_stock36_optimum_usage_tbl.
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

define encryptDecrypt com.cablevision.edh.udf.EncryptDecryptUtil();
define replaceLeadingZero com.cablevision.edh.udf.ReplaceLeadingZero();

smith_hit_data_stock36 = 
	LOAD '${hive_database_name_gold}.${gold_stock36_hit_data_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

--##############################################################################
--#                                 Filter                                     #
--##############################################################################

smith_hit_data_stock36_filter = 
    FILTER smith_hit_data_stock36 
    BY (suite_name=='$suite_name' AND source_date=='$source_date')
;
    
--##############################################################################
--#                                 Decrypt Data                               #
--##############################################################################

hit_data_raw_decrypted_records = 
	FOREACH smith_hit_data_stock36_filter 
	GENERATE accept_language,
	(account_number == '-1'  ? account_number : encryptDecrypt('$suite_name', 'ACC_NUM','$username',account_number, 'DECRYPT', '$namenode_service')) AS account_number,
	browser_id..
	last_purchase_time_gmt,
	encryptDecrypt('$suite_name', 'MAC_ID','$username',mac_id, 'DECRYPT', '$namenode_service') AS mac_id,
	monthly_visitor..
;

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

generate_records = FOREACH hit_data_raw_decrypted_records GENERATE
    account_number AS account_number,
	browser_id AS browser_id,
	prop2 AS campaign,
	connection_type_id AS connection_type_id,
	(account_number == '-1' ? account_number : replaceLeadingZero(SUBSTRING(account_number,0,5))) AS corp,
	country_id AS country_id,
	(account_number == '-1' ? account_number : replaceLeadingZero(SUBSTRING(account_number,11,13))) AS cust,
	CurrentTime()   AS  dtm_created,
	CurrentTime() AS dtm_last_updated,
	(account_number == '-1' ? account_number : replaceLeadingZero(SUBSTRING(account_number,5,11))) AS dwelling_nbr,
	isp_id  AS isp_id,
	javascript_version_id  AS javascript_version_id,
	language_id   AS language_id,
	CurrentTime() AS last_updated_by_system,
	mac_id   AS  mac_address,
	monthly_visitor AS monthly_visitor,
	operating_system_id AS operating_system_id,
	page_name_id AS page_name_id,
	page_type AS page_type ,
	page_url AS page_url ,   
	plugins AS plugins , 
	search_engine_id AS search_engine_id, 
	site_section_id AS site_section_id  ,        
	'omniture' AS sourced_from_system    	, 
	'$suite_id' AS suite_id,
	visid_high AS visid_high, 
	visid_low AS visid_low,
	SUBSTRING(ToString(date_time),0,10) AS visit_date,   
	visit_num  AS visit_num,
	visit_start_time_gmt AS visit_start_time_gmt,
	visit_page_num  AS visit_page_num,	
	prop7 AS vod_category,
	prop9 AS vod_title,
	prop23 AS vod_play_time,
	prop14 AS void_id;
	
--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE generate_records
   INTO '${hive_database_name_smith}.${smith_kom_stock36_optimum_usage}' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('source_date=$source_date')
; 
	
--##############################################################################
--#                              End                                           #
--##############################################################################			