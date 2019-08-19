--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from incoming_hit_data table to gold_opt_hit_data table
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
define ResolvePluginId com.cablevision.edh.udf.ResolvePluginId();
define dateDifferCount com.cablevision.edh.udf.DateDifferCount();
define Stitch org.apache.pig.piggybank.evaluation.Stitch;
define Over org.apache.pig.piggybank.evaluation.Over('int');



incoming_hit_data = 
	LOAD '${hive_database_name_incoming}.${incoming_hit_data_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
gold_h_optimum_user = 
	LOAD '${hive_database_name_gold_ods}.${gold_h_optimum_user_tbl}' 
    USING org.apache.hive.hcatalog.pig.HCatLoader()
;	
	   
incoming_browser = 
	LOAD '${hive_database_name_incoming}.${incoming_browser_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_connection_type = 
	LOAD '${hive_database_name_incoming}.${incoming_connection_type_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_country = 
	LOAD '${hive_database_name_incoming}.${incoming_country_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_javascript_version = 
	LOAD '${hive_database_name_incoming}.${incoming_javascript_version_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_languages = 
	LOAD '${hive_database_name_incoming}.${incoming_languages_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_operating_systems = 
	LOAD '${hive_database_name_incoming}.${incoming_operating_systems_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_plugins = 
	LOAD '${hive_database_name_incoming}.${incoming_plugins_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_referrer_type = 
	LOAD '${hive_database_name_incoming}.${incoming_referrer_type_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
incoming_search_engines = 
	LOAD '${hive_database_name_incoming}.${incoming_search_engines_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_optimum_isp = 
    LOAD '${hive_database_name_incoming}.${incoming_optimum_isp_tbl}'
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
					
incoming_optimum_pagenames = 
    LOAD '${hive_database_name_incoming}.${incoming_optimum_pagenames_tbl}'
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
					
incoming_optimum_site_section = 
    LOAD '${hive_database_name_incoming}.${incoming_optimum_site_sections_tbl}'
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
   
incoming_optimum_error_message = 
    LOAD '${hive_database_name_incoming}.${incoming_optimum_error_message_tbl}'
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

--##############################################################################
--#                                 Filter                                     #
--##############################################################################

incoming_hit_data_filter = 
	FILTER incoming_hit_data 
    BY (suite_name=='$suite_name' AND source_date == '$source_date' )
;

gold_h_optimum_user_a = foreach  gold_h_optimum_user generate h_optimum_user_id,optimum_user_id,customer_account_id,ToString(dtm_efftv,'yyyy-MM-dd') as dtm_efftv,ToString(dtm_expired,'yyyy-MM-dd') as dtm_expired,dtm_created..source_appl;

gold_h_optimum_user_active = 
   FILTER gold_h_optimum_user_a BY       
       dateDifferCount('$source_date',dtm_efftv) >=0   
           AND
       dateDifferCount(dtm_expired,'$source_date') >= 0
;  


gold_h_optimum_user_active_group = 
   GROUP gold_h_optimum_user_active BY (optimum_id)
;

gold_h_optimum_user_active_derive_row_number = 
   FOREACH gold_h_optimum_user_active_group
   {
      gold_h_optimum_user_active_order = 
         ORDER gold_h_optimum_user_active 
         BY dtm_efftv DESC,
         h_optimum_user_id DESC
      ;
      GENERATE 
      FLATTEN(Stitch(gold_h_optimum_user_active_order,Over(gold_h_optimum_user_active_order,'row_number')));
   }
;


gold_h_optimum_user_active_derive_row_number_generated = 
   FOREACH gold_h_optimum_user_active_derive_row_number 
   GENERATE
   stitched::h_optimum_user_id         AS h_optimum_user_id,
   stitched::optimum_user_id           AS optimum_user_id,
   stitched::customer_account_id       AS customer_account_id,
   stitched::dtm_efftv                 AS dtm_efftv,
   stitched::dtm_expired               AS dtm_expired,
   stitched::dtm_created               AS dtm_created,
   stitched::created_by_process        AS created_by_process,
   stitched::sourced_from_system       AS sourced_from_system,
   stitched::last_updated_by_process   AS last_updated_by_process,
   stitched::last_updated_from_system  AS last_updated_from_system,
   stitched::dtm_last_updated          AS dtm_last_updated,		
   stitched::dtm_last_modified         AS dtm_last_modified,		
   stitched::id_cust                   AS id_cust,					
   stitched::id_service_rec            AS id_service_rec,			
   stitched::id_user                   AS id_user,
   stitched::corp                      AS corp,
   stitched::dwelling_nbr              AS dwelling_nbr,
   stitched::cust                      AS cust,
   stitched::dtm_start_date            AS dtm_start_date,
   stitched::dtm_end_date              AS dtm_end_date,
   stitched::email_ind                 AS email_ind,
   stitched::id_domain                 AS id_domain,
   stitched::optimum_id                AS optimum_id,
   stitched::mixed_case_optimum_id     AS mixed_case_optimum_id,
   stitched::id_user_type              AS id_user_type,
   stitched::user_type_descr           AS user_type_descr,
   stitched::source_appl               AS source_appl,
   stitched::result                    AS row_number
;

gold_h_optimum_user_active_unique = 
   FILTER gold_h_optimum_user_active_derive_row_number_generated BY (row_number == 1)
;

gold_h_optimum_user_generated = 
	FOREACH gold_h_optimum_user_active_unique 
	GENERATE corp       AS corp, 
		dwelling_nbr    AS dwelling_nbr, 
		cust            AS cust, 
		optimum_user_id AS optimum_user_id, 
		optimum_id      AS optimum_id, 
		dtm_efftv       AS dtm_efftv, 
		dtm_expired     AS dtm_expired
;

--Filter lookup data by suite_id   
incoming_browser_filter = 
	FILTER incoming_browser 
	BY  (suite_id=='$suite_id')
;
	
incoming_connection_type_filter = 
	FILTER incoming_connection_type 
	BY (suite_id=='$suite_id')
;
	
incoming_country_filter = 
	FILTER incoming_country
	BY (suite_id=='$suite_id')
;
	
incoming_javascript_version_filter = 
	FILTER incoming_javascript_version 
	BY (suite_id=='$suite_id')
;
	
incoming_languages_filter = 
	FILTER incoming_languages 
	BY (suite_id=='$suite_id')
;
	
incoming_operating_systems_filter = 
	FILTER incoming_operating_systems 
	BY (suite_id=='$suite_id')
;
	
incoming_plugins_filter = 
	FILTER incoming_plugins 
	BY (suite_id=='$suite_id')
;
	
incoming_referrer_type_filter = 
	FILTER incoming_referrer_type 
	BY (suite_id=='$suite_id')
;
	
incoming_search_engines_filter = 
	FILTER incoming_search_engines 
	BY (suite_id=='$suite_id')
;
	
incoming_optimum_isp_filter = 
	FILTER incoming_optimum_isp 
	BY (suite_id=='$suite_id')
;
	
incoming_optimum_pagenames_filter = 
	FILTER incoming_optimum_pagenames 
	BY (suite_id=='$suite_id')
;
	
incoming_optimum_site_section_filter  = 
	FILTER incoming_optimum_site_section 
	BY (suite_id=='$suite_id')
;  

incoming_optimum_error_message_filter  = 
	FILTER incoming_optimum_error_message 
	BY (suite_id=='$suite_id')
;

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

	SPLIT incoming_hit_data_filter 
		INTO hit_data_with_mandatory_records
	   	IF (
	   		browser is not null  
	   		AND connection_type is not null  
	   		AND date_time is not null 
	   		AND TRIM(domain) is not null AND TRIM(domain) !='' 
	        AND mobile_id is not null 
	        AND os is not null  
	        AND TRIM(page_url) is not null AND TRIM(page_url) !=''  
	        AND TRIM(post_visid_high) is not null AND TRIM(post_visid_high) !='' 
	        AND TRIM(post_visid_low) is not null AND TRIM(post_visid_low) !='' 
	        AND ref_type is not null 
	        AND search_engine is not null         
	        AND visit_num is not null 
	        AND visit_page_num is not null 
	        ),
		hit_data_missing_mandatory_records OTHERWISE
	;
	
	hit_data_invalid_records_mandatory_field = 
		FOREACH hit_data_missing_mandatory_records 
		GENERATE 'Mandatory field missing' as error_description,
		accept_language..
	;
						  				
	SPLIT hit_data_with_mandatory_records 
		INTO hit_data_valid_records
	    IF (
	    	(long)browser_height is not null 
	        AND (long)browser_width is not null  
	        AND (long)click_action_type is not null 
	        AND (long)click_context_type is not null 
	        AND (long)click_sourceid is not null 
	        AND (long)color is not null 
	        AND (biginteger)post_visid_high is not null 
	        AND (biginteger)post_visid_low is not null
			),
		hit_data_invalid_records OTHERWISE
	;
						      
	hit_data_invalid_non_numeric_records = 
		FOREACH hit_data_invalid_records 
		GENERATE 'Contain non-numeric value' as  error_description,
		accept_language..
	;
							         						         
	union_error_records = 
		UNION ONSCHEMA hit_data_invalid_records_mandatory_field,
		hit_data_invalid_non_numeric_records
	;

--##############################################################################
--#                                 Decrypt Data                               #
--##############################################################################	
	
hit_data_decrypted_records = 
	FOREACH hit_data_valid_records 
	GENERATE
	   ..duplicate_purchase,
	   (TRIM(evar1) == '' OR evar1 is null ? pagename : evar1 )    AS evar1_pagename,
	   evar2
	   ..
	   evar22,
	   encryptDecrypt('$suite_name', 'TTT_ACCT_NUMBER','$username', evar23, 'DECRYPT','$namenode_service') AS evar23_ttt_acct_number,
	   evar24,
	   evar25,
	   evar26,
	   evar27
	   ..	
       prop9,
	   prop10,
	   encryptDecrypt('$suite_name', 'OPTIMUM_ID','$username', prop11, 'DECRYPT','$namenode_service') AS prop11_optimum_id,
	   prop12
	   ..
	   prop24,
	   prop25 AS prop25_hulu_id,
	   prop26
	   ..
	   prop34,
	   prop35 AS prop35_opt_err_msg_desc,
	   prop36..
;
		
--##############################################################################
--#         Generate Corp, Dwelling_nrb, Cust,optimum_user_id                  #
--##############################################################################

SPLIT hit_data_decrypted_records 
	INTO hit_data_decrypted_records_missing_optimumId
   	IF ( 
   	TRIM(prop11_optimum_id) is null 
   	),
    hit_data_decrypted_records_with_optimumId OTHERWISE
;
		
		
		
hit_data_decrypted_records_with_optimumId_join =
   JOIN hit_data_decrypted_records_with_optimumId BY prop11_optimum_id LEFT,
   gold_h_optimum_user_generated BY optimum_id;

hit_data_records_with_optimumId_generated = 
	FOREACH hit_data_decrypted_records_with_optimumId_join
	GENERATE		
        accept_language
		..
		prop10,
		hit_data_decrypted_records_with_optimumId::prop11_optimum_id as prop11_optimum_id,
        prop12
		..
		zip,
		(gold_h_optimum_user_generated::corp is null ? -1 : gold_h_optimum_user_generated::corp) AS corp,
		(TRIM(gold_h_optimum_user_generated::cust) == '' 
		   OR gold_h_optimum_user_generated::cust is null ? '-1' : gold_h_optimum_user_generated::cust) AS cust,
 		(TRIM(gold_h_optimum_user_generated::dwelling_nbr) == '' 
		   OR gold_h_optimum_user_generated::dwelling_nbr is null ? '-1' : gold_h_optimum_user_generated::dwelling_nbr) AS dwelling_nbr,
  		(gold_h_optimum_user_generated::optimum_user_id is null ? -1 : gold_h_optimum_user_generated::optimum_user_id) AS optimum_user_id
;
		
SPLIT hit_data_decrypted_records_missing_optimumId 
	INTO hit_data_decrypted_records_missing_optimumId_missing_ttt_acct_number
   	IF ( 
   	TRIM(evar23_ttt_acct_number) is null   
   	),
    hit_data_decrypted_records_missing_optimumId_with_ttt_acct_number OTHERWISE
;
    
hit_data_decrypted_records_missing_optimumId_missing_ttt_acct_number_generated = 
	FOREACH hit_data_decrypted_records_missing_optimumId_missing_ttt_acct_number 
	GENERATE        
		accept_language
		..		
		zip,
		-1   AS corp,
		'-1' AS cust,
		'-1' AS dwelling_nbr,
		-1   AS optimum_user_id
; 
		
hit_data_decrypted_records_missing_optimumId_with_ttt_acct_number_generated = 
	FOREACH hit_data_decrypted_records_missing_optimumId_with_ttt_acct_number 
	GENERATE        
		accept_language
		..		
		zip,
		SUBSTRING(evar23_ttt_acct_number, 1, 5)  AS corp,
		SUBSTRING(evar23_ttt_acct_number, 6, 6)  AS cust,
		SUBSTRING(evar23_ttt_acct_number, 12, 2) AS dwelling_nbr,
		-1                                                                  AS optimum_user_id
;
		   		   		
hit_data_records = 
	UNION ONSCHEMA hit_data_records_with_optimumId_generated, 
	hit_data_decrypted_records_missing_optimumId_missing_ttt_acct_number_generated, 
	hit_data_decrypted_records_missing_optimumId_with_ttt_acct_number_generated
;
	
--##############################################################################
--#         			Join with look-up                                      #
--##############################################################################
	
hit_data_records_join_browser = 
	JOIN hit_data_records BY browser LEFT, 
    incoming_browser_filter BY browser_id
    USING 'replicated'
;

hit_data_records_join_connection_type = 
	JOIN hit_data_records_join_browser BY connection_type LEFT, 
	incoming_connection_type_filter BY connection_type_id
	USING 'replicated'
;
									
hit_data_records_join_country = 
	JOIN hit_data_records_join_connection_type BY country LEFT,
    incoming_country_filter BY country_id
    USING 'replicated'
;
						
hit_data_records_join_javascript_version = 
	JOIN hit_data_records_join_country BY javascript LEFT,
    incoming_javascript_version_filter BY javascript_version_id
    USING 'replicated'
;
							   
hit_data_records_join_languages = 
	JOIN hit_data_records_join_javascript_version BY language LEFT,
    incoming_languages_filter BY language_id
    USING 'replicated'
;
						
hit_data_records_join_operating_systems = 
	JOIN hit_data_records_join_languages BY os LEFT,
	incoming_operating_systems_filter BY operating_system_id
	USING 'replicated'
;
						
hit_data_records_join_referrer_type = 
	JOIN hit_data_records_join_operating_systems BY ref_type LEFT,
    incoming_referrer_type_filter BY referrer_type_id
    USING 'replicated'
;	
						   
hit_data_records_join_search_engines = 
	JOIN hit_data_records_join_referrer_type BY search_engine LEFT,
	incoming_search_engines_filter BY search_engine_id
	USING 'replicated'
;

hit_data_records_join_isp = 
	JOIN hit_data_records_join_search_engines BY domain LEFT,
	incoming_optimum_isp_filter BY isp_name
	USING 'replicated'
;
	
hit_data_records_join_pagenames = 
	JOIN hit_data_records_join_isp BY evar1_pagename LEFT,
    incoming_optimum_pagenames_filter BY pagename
    USING 'replicated'
;
		
hit_data_records_join_site_section = 
	JOIN hit_data_records_join_pagenames BY channel LEFT,
	incoming_optimum_site_section_filter BY site_section_name
	USING 'replicated'
;

hit_data_records_join_error_message = 
	JOIN hit_data_records_join_site_section BY prop35_opt_err_msg_desc LEFT,
	incoming_optimum_error_message_filter BY opt_err_msg_desc
	USING 'replicated'
;

incoming_plugins_tuples = 
   FOREACH incoming_plugins_filter 
   GENERATE TOTUPLE(*) AS (plugin_recs:tuple())
;

incoming_plugins_tbl_filter_all = 
   GROUP incoming_plugins_tuples ALL
;

incoming_plugins_generate = 
   FOREACH incoming_plugins_tbl_filter_all 
   GENERATE TOBAG(*) AS plugin_rec_bag
;

hit_data_records_join_plugins = 
   FOREACH hit_data_records_join_error_message 
   GENERATE ..persistent_cookie,
              ResolvePluginId(plugins, incoming_plugins_generate.plugin_rec_bag) AS plugin_name,
              plugins..
;
	
generated_records = 
	FOREACH hit_data_records_join_plugins 
	GENERATE
        accept_language         AS accept_language,
		browser_height          AS browser_height,
        browser                 AS browser_id,
        hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::hit_data_records_join_browser::incoming_browser_filter::browser_name AS browser_name,
		browser_width           AS browser_width,	
		c_color                 AS c_color,
		campaign                AS campaign,		
		click_action            AS click_action,
		click_action_type       AS click_action_type,
		click_context           AS click_context,
		click_context_type      AS click_context_type,
		click_sourceid          AS click_sourceid,
		click_tag               AS click_tag,
		code_ver                AS code_ver,
		color                   AS color,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::incoming_connection_type_filter::connection_type AS connection_type,
		hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::hit_data_records_join_browser::hit_data_records::connection_type AS connection_type_id,
		cookies 				AS cookies,
		(int)corp               AS corp,		
		country                 AS country_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::incoming_country_filter::country_name AS country_name,
		ct_connect_type         AS ct_connect_type,
		curr_factor             AS curr_factor,
		curr_rate               AS curr_rate,
		currency                AS currency,
        (chararray)cust    		AS cust,		
		cust_hit_time_gmt       AS cust_hit_time_gmt,
		cust_visid              AS cust_visid,
		prop10                  AS custom_link,
		daily_visitor           AS daily_visitor,
		CurrentTime()           AS dtm_created,	
		duplicate_events        AS duplicate_events,
		duplicate_purchase      AS duplicate_purchase,		
		duplicated_from         AS duplicated_from,
		(chararray)dwelling_nbr AS dwelling_nbr,
		evar1_pagename          AS evar1_pagename,
		evar10                  AS evar10,
		evar11                  AS evar11,
		evar12                  AS evar12,
		evar13         			AS evar13,
		evar14         			AS evar14,
		evar15         			AS evar15,
		evar16         			AS evar16,  
		evar17         			AS evar17,
		evar18         			AS evar18,
		evar19         			AS evar19,
		evar2                   AS evar2,
		evar20         			AS evar20,
		evar21         			AS evar21,
		evar22         			AS evar22,
		evar23_ttt_acct_number  AS evar23_ttt_acct_number,
		evar24                  AS evar24_alerts_notification,
		evar25                  AS evar25,
		evar26                  AS evar26_tactic,
		evar27                  AS evar27,
		evar28         			AS evar28,
		evar29         			AS evar29,		
		evar3                   AS evar3,
		evar30         			AS evar30,
		evar31         			AS evar31,
		evar32         			AS evar32,
		evar33         			AS evar33,
		evar34         			AS evar34,
		evar35         			AS evar35,
		evar36         			AS evar36,
		evar37         			AS evar37,
		evar38         			AS evar38,
		evar39         			AS evar39,
		evar4                   AS evar4,
		evar40         			AS evar40,
		evar41         			AS evar41,
		evar42                  AS evar42_campaign_evar,
		evar43                  AS evar43_wildcard,
        evar44                  AS evar44,
        evar45         			AS evar45,
		evar46         			AS evar46,
		evar47         			AS evar47,
		evar48         			AS evar48,
		evar49         			AS evar49,
		evar5                   AS evar5,
		evar50         			AS evar50,
		evar51         			AS evar51,
		evar52        			AS evar52,
		evar53         			AS evar53,
		evar54         			AS evar54,
		evar55        			AS evar55,
		evar56         			AS evar56,
		evar57         			AS evar57,
		evar58         			AS evar58,
		evar59         			AS evar59,		
		evar6                   AS evar6,
		evar60         			AS evar60,
		evar61         			AS evar61,
		evar62         			AS evar62,
		evar63         			AS evar63,
		evar64         			AS evar64,
		evar65         			AS evar65,
		evar66         			AS evar66,
		evar67         			AS evar67,
		evar68         			AS evar68,
		evar69         			AS evar69,
		evar7                   AS evar7,
		evar70         			AS evar70,
		evar71         			AS evar71,
		evar72         			AS evar72,
		evar73         			AS evar73,
		evar74         			AS evar74,
		evar75         			AS evar75,
		evar8                   AS evar8,
		evar9                   AS evar9,
		event_list         		AS event_list,
		exclude_hit         	AS exclude_hit,
		first_hit_page_url      AS first_hit_page_url,
		first_hit_pagename      AS first_hit_pagename,		
		first_hit_referrer      AS first_hit_referrer,
		first_hit_time_gmt      AS first_hit_time_gmt,
		geo_city         		AS geo_city,
		geo_country         	AS geo_country,
		geo_dma                 AS geo_dma,
		geo_region              AS geo_region,
		geo_zip                 AS geo_zip,
		hier1                   AS hier1,
		hier2         			AS hier2,
		hier3         			AS hier3,
		hier4         			AS hier4,
		hier5         			AS hier5,
		hit_source              AS hit_source,
		hit_time_gmt         	AS hit_time_gmt,		
		hitid_high         		AS hitid_high,
		hitid_low         		AS hitid_low,		
		homepage         		AS homepage,
		hourly_visitor          AS hourly_visitor,		
		ip         				AS ip,          			
		ip2         			AS ip2,    
		hit_data_records_join_pagenames::hit_data_records_join_isp::incoming_optimum_isp_filter::isp_id AS isp_id,
		domain                  AS isp_name,
		j_jscript               AS j_jscript,
		java_enabled         	AS java_enabled,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::incoming_javascript_version_filter::javascript_version AS javascript_version,
		javascript              AS javascript_version_id,		
		language                AS language_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::hit_data_records_join_languages::incoming_languages_filter::language_name AS language_name,		
		last_hit_time_gmt       AS last_hit_time_gmt,
		last_purchase_num       AS last_purchase_num,       
		last_purchase_time_gmt  AS last_purchase_time_gmt, 
		mobile_id         		AS mobile_id,
		monthly_visitor         AS monthly_visitor,
		mvvar1         			AS mvvar1,
		mvvar2         			AS mvvar2,          		
		mvvar3         			AS mvvar3,          		
		namespace         		AS namespace,
		new_visit               AS new_visit,
		os                      AS operating_system_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::incoming_operating_systems_filter::operating_system_name AS operating_system_name,		
		incoming_optimum_error_message_filter::opt_err_msg_id AS opt_err_msg_id,		
		optimum_user_id      	AS optimum_user_id,
		p_plugins               AS p_plugins,
		page_event              AS page_event,
		page_event_var1         AS page_event_var1,         
		page_event_var2         AS page_event_var2,         
		page_event_var3         AS page_event_var3,
		hit_data_records_join_pagenames::incoming_optimum_pagenames_filter::page_name_id AS page_name_id,
		page_type         		AS page_type,          	
		page_url         		AS page_url,          			
		hit_data_records_join_site_section::hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::hit_data_records_join_browser::hit_data_records::pagename as pagename,
		
		paid_search         	AS paid_search,          
		partner_plugins         AS partner_plugins,         
		persistent_cookie       AS persistent_cookie,   
		plugin_name             AS plugin_name,
		plugins         		AS plugins,          	
		post_browser_height     AS post_browser_height,     
		post_browser_width      AS post_browser_width,      
		post_campaign         	AS 	post_campaign ,         
		post_channel         	AS 	post_channel,          
		post_cookies         	AS 	post_cookies,          
		post_currency         	AS 	post_currency,          
		post_cust_hit_time_gmt  AS post_cust_hit_time_gmt,  
		post_cust_visid         AS post_cust_visid,         
		post_evar1         		AS post_evar1,          	
		post_evar10         	AS post_evar10,          
		post_evar11         	AS post_evar11,          
		post_evar12         	AS post_evar12,          
		post_evar13         	AS post_evar13,          
		post_evar14         	AS post_evar14,          
		post_evar15         	AS post_evar15,          
		post_evar16         	AS post_evar16,          
		post_evar17         	AS post_evar17,          
		post_evar18         	AS post_evar18,          
		post_evar19         	AS post_evar19,
		post_evar2         		AS post_evar2, 
        post_evar20         	AS post_evar20,          
		post_evar21         	AS post_evar21,          
		post_evar22         	AS post_evar22,          
		post_evar23         	AS post_evar23,          
		post_evar24         	AS post_evar24,          
		post_evar25         	AS post_evar25,          
		post_evar26         	AS post_evar26,          
		post_evar27         	AS post_evar27,          
		post_evar28         	AS post_evar28,          
		post_evar29         	AS post_evar29,		
		post_evar3         		AS post_evar3,  
        post_evar30         	AS post_evar30,          
		post_evar31         	AS post_evar31,          
		post_evar32         	AS post_evar32,          
		post_evar33         	AS post_evar33,          
		post_evar34         	AS post_evar34,          
		post_evar35         	AS post_evar35,          
		post_evar36         	AS post_evar36,          
		post_evar37         	AS post_evar37,          
		post_evar38         	AS post_evar38,          
		post_evar39         	AS post_evar39,          		
		post_evar4         		AS post_evar4,
        post_evar40         	AS post_evar40,          
		post_evar41         	AS post_evar41,          
		post_evar42         	AS post_evar42,          
		post_evar43         	AS post_evar43,          
		post_evar44         	AS post_evar44,          
		post_evar45         	AS post_evar45,          
		post_evar46         	AS post_evar46,         
		post_evar47         	AS post_evar47,         
		post_evar48         	AS post_evar48,         
		post_evar49         	AS post_evar49,		
		post_evar5         		AS post_evar5,          	
		post_evar50         	AS post_evar50,         
		post_evar51         	AS post_evar51,         
		post_evar52         	AS post_evar52,         
		post_evar53         	AS post_evar53,         
		post_evar54         	AS post_evar54,         
		post_evar55         	AS post_evar55,         
		post_evar56         	AS post_evar56,         
		post_evar57         	AS post_evar57,         
		post_evar58         	AS post_evar58,         
		post_evar59        		AS post_evar59,     
		post_evar6         		AS post_evar6,          	
		post_evar60         	AS post_evar60,         
		post_evar61         	AS post_evar61,         
		post_evar62         	AS post_evar62,         
		post_evar63         	AS post_evar63,         
		post_evar64         	AS post_evar64,         
		post_evar65         	AS post_evar65,         
		post_evar66         	AS post_evar66,         
		post_evar67         	AS post_evar67,         
		post_evar68         	AS post_evar68,         
		post_evar69         	AS post_evar69,         
		post_evar7         		AS post_evar7,          	
		post_evar70         	AS post_evar70,         
		post_evar71         	AS post_evar71,         
		post_evar72        	 	AS post_evar72,         
		post_evar73         	AS post_evar73,         
		post_evar74         	AS post_evar74,         
		post_evar75         	AS post_evar75,         
		post_evar8         		AS post_evar8,          	
		post_evar9         		AS post_evar9,          	
		post_event_list         AS post_event_list,
		post_hier1         		AS post_hier1,
		post_hier2         		AS post_hier2,
		post_hier3         		AS post_hier3,
		post_hier4         		AS post_hier4,
		post_hier5         		AS post_hier5,
		post_java_enabled       AS post_java_enabled,
		post_keywords           AS post_keywords,
		post_mvvar1             AS post_mvvar1,
		post_mvvar2             AS post_mvvar2,
		post_mvvar3             AS post_mvvar3,
		post_page_event         AS post_page_event,
		post_page_event_var1    AS post_page_event_var1,
		post_page_event_var2    AS post_page_event_var2,
		post_page_event_var3    AS post_page_event_var3,
		post_page_type          AS post_page_type,
		post_page_url           AS post_page_url,
		post_pagename           AS post_pagename,
		post_pagename_no_url    AS post_pagename_no_url,		
		post_partner_plugins    AS post_partner_plugins,
		post_persistent_cookie  AS post_persistent_cookie,
		post_product_list       AS post_product_list,
		post_prop1         		AS post_prop1,
		post_prop10	         	AS post_prop10,
		post_prop11         	AS post_prop11,        
		post_prop12         	AS post_prop12,        
		post_prop13         	AS post_prop13,        
		post_prop14         	AS post_prop14,        
		post_prop15         	AS post_prop15,        
		post_prop16         	AS post_prop16,        
		post_prop17         	AS post_prop17,        
		post_prop18         	AS post_prop18,        
		post_prop19         	AS post_prop19,       
		post_prop2         		AS post_prop2,
		post_prop20         	AS post_prop20,        
		post_prop21         	AS post_prop21,        
		post_prop22         	AS post_prop22,        
		post_prop23         	AS post_prop23,        
		post_prop24         	AS post_prop24,        
		post_prop25         	AS post_prop25,        
		post_prop26         	AS post_prop26,        
		post_prop27         	AS post_prop27,        
		post_prop28         	AS post_prop28,        
		post_prop29         	AS post_prop29,        
		post_prop3         		AS post_prop3,
		post_prop30         	AS post_prop30,        
		post_prop31         	AS post_prop31,       
		post_prop32         	AS post_prop32,        
		post_prop33         	AS post_prop33,        
		post_prop34         	AS post_prop34,        
		post_prop35         	AS post_prop35,        
		post_prop36         	AS post_prop36,        
		post_prop37         	AS post_prop37,        
		post_prop38         	AS post_prop38,        
		post_prop39         	AS post_prop39,        
		post_prop4         		AS post_prop4,
		post_prop40         	AS post_prop40,        
		post_prop41         	AS post_prop41,        
		post_prop42         	AS post_prop42,        
		post_prop43         	AS post_prop43,        
		post_prop44         	AS post_prop44,        
		post_prop45         	AS post_prop45,        
		post_prop46         	AS post_prop46,        
		post_prop47         	AS post_prop47,        
		post_prop48         	AS post_prop48,        
		post_prop49         	AS post_prop49,        
		post_prop5         		AS post_prop5,
		post_prop50         	AS post_prop50,        
		post_prop51         	AS post_prop51,        
		post_prop52         	AS post_prop52,        
		post_prop53         	AS post_prop53,        
		post_prop54         	AS post_prop54,        
		post_prop55         	AS post_prop55,        
		post_prop56         	AS post_prop56,        
		post_prop57         	AS post_prop57,        
		post_prop58         	AS post_prop58,        
		post_prop59         	AS post_prop59,        
		post_prop6         		AS post_prop6,
		post_prop60         	AS post_prop60,        
		post_prop61         	AS post_prop61,        
		post_prop62         	AS post_prop62,        
		post_prop63         	AS post_prop63,        
		post_prop64         	AS post_prop64,       
		post_prop65         	AS post_prop65,        
		post_prop66        		AS post_prop66,    
		post_prop67         	AS post_prop67,        
		post_prop68         	AS post_prop68,        
		post_prop69         	AS post_prop69,
		post_prop7         		AS post_prop7,
		post_prop70         	AS post_prop70,        
		post_prop71         	AS post_prop71,        
		post_prop72         	AS post_prop72,        
		post_prop73         	AS post_prop73,        
		post_prop74         	AS post_prop74,        
		post_prop75         	AS post_prop75,        
		post_prop8        		AS post_prop8,
		post_prop9         		AS post_prop9,
		post_purchaseid         AS post_purchaseid,
		post_referrer         	AS post_referrer,
		post_search_engine      AS post_search_engine,      
		post_state         		AS post_state,      
		post_survey         	AS post_survey,
		post_t_time_info        AS post_t_time_info,
		post_tnt         		AS post_tnt,        
		post_transactionid      AS post_transactionid,      		        
		post_visid_high   		as post_visid_high ,
		post_visid_low  		as post_visid_low ,
		post_visid_type         AS post_visid_type,         
		post_zip         		AS post_zip,         		        
		prev_page         		AS prev_page,        
		product_list         	AS product_list,     
		product_merchandising   AS product_merchandising,
		prop1         			AS prop1,
		prop11_optimum_id       AS prop11_optimum_id,
		prop12         			AS prop12,
		prop13         			AS prop13,
		prop14         			AS prop14,
		prop15         			AS prop15,
		prop16         			AS prop16,
		prop17         			AS prop17,
		prop18         			AS prop18,
		prop19         			AS prop19,
		prop2         			AS prop2,
		prop20         			AS prop20,
		prop21         			AS prop21,
		prop22        			AS prop22,
		prop23        			AS prop23,
		prop24        			AS prop24,
		prop25_hulu_id          AS prop25_hulu_id,
		prop26                  AS prop26,
		prop27        			AS prop27,
		prop28        			AS prop28,
		prop29        			AS prop29,
		prop3         			AS prop3,
		prop30        			AS prop30,
		prop31        			AS prop31,
		prop32        			AS prop32,
		prop33        			AS prop33,
		prop34        			AS prop34,		
		prop35_opt_err_msg_desc AS prop35_opt_err_msg_desc,
		prop36                  AS prop36,
		prop37        			AS prop37,
		prop38        			AS prop38,
		prop39        			AS prop39,
		prop4         			AS prop4,
		prop40        			AS prop40,
		prop41        			AS prop41,
		prop42        			AS prop42,
		prop43        			AS prop43,
		prop44        			AS prop44,
		prop45        			AS prop45,
		prop46        			AS prop46,
		prop47        			AS prop47,
		prop48        			AS prop48,
		prop49        			AS prop49,
		prop5         			AS prop5,
		prop50        			AS prop50,
		prop51        			AS prop51,
		prop52        			AS prop52,
		prop53        			AS prop53,
		prop54        			AS prop54,
		prop55        			AS prop55,
		prop56        			AS prop56,
		prop57        			AS prop57,
		prop58        			AS prop58,
		prop59        			AS prop59,
		prop6                   AS prop6,
		prop60        			AS prop60,
		prop61        			AS prop61,
		prop62        			AS prop62,
		prop63        			AS prop63,
		prop64        			AS prop64,
		prop65        			AS prop65,
		prop66        			AS prop66,
		prop67        			AS prop67,
		prop68        			AS prop68,
		prop69        			AS prop69,
		prop7                   AS prop7,
		prop70        			AS prop70,
		prop71        			AS prop71,
		prop72        			AS prop72,
		prop73        			AS prop73,
		prop74        			AS prop74,
		prop75        			AS prop75,
		prop8                   AS prop8,
		prop9         			AS prop9,		
		purchaseid         		AS purchaseid,
		quarterly_visitor       AS quarterly_visitor,       
		ref_domain	            AS ref_domain,
		referrer                AS referrer,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::incoming_referrer_type_filter::referrer_type_fullname AS   referrer_type_fullname,
		ref_type                AS referrer_type_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_referrer_type::incoming_referrer_type_filter::referrer_type_shortname AS referrer_type_shortname,
        resolution              AS resolution,
		s_resolution         	AS s_resolution, 
		sampled_hit             AS sampled_hit,
		search_engine           AS search_engine_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::incoming_search_engines_filter::search_engine_name AS search_engine_name,
		search_page_num         AS search_page_num,
		secondary_hit  			AS secondary_hit,
		service         		AS service,         
		incoming_optimum_site_section_filter::site_section_id AS site_section_id, 
		channel                 AS site_section_name,
		'omniture'              AS sourced_from_system,
		sourceid         		AS sourceid,     		
		state         			AS state,        
		stats_server         	AS stats_server, 
		$suite_id               AS suite_id,
		t_time_info       		AS t_time_info,
		tnt         			AS tnt,
		tnt_post_vista          AS tnt_post_vista,          
		transactionid           AS transactionid,           
		truncated_hit           AS truncated_hit,          		
		ua_color    			AS ua_color,
		ua_os         			AS ua_os,
		ua_pixels         		AS ua_pixels,
		user_agent         		AS user_agent,
		user_hash         		AS user_hash,
		user_server         	AS user_server,
		userid         			AS userid,
		username         		AS username,		
		va_closer_detail        AS va_closer_detail,
		va_closer_id            AS va_closer_id,
		va_finder_detail        AS va_finder_detail,
		va_finder_id            AS va_finder_id,
		va_instance_event       AS va_instance_event,
		va_new_engagement       AS va_new_engagement,
		visid_high         		AS visid_high,
		visid_low         		AS visid_low,
		visid_new         		AS visid_new,
		visid_timestamp	        AS visid_timestamp,
		visid_type         		AS visid_type,
		date_time               AS visit_date,
		visit_keywords          AS visit_keywords,
		visit_num         		AS visit_num,
		visit_page_num          AS visit_page_num,
		visit_referrer          AS visit_referrer,
		visit_search_engine     AS visit_search_engine,		
		visit_start_page_url    AS visit_start_page_url,
		visit_start_pagename    AS visit_start_pagename,
		visit_start_time_gmt    AS visit_start_time_gmt,
		weekly_visitor          AS weekly_visitor,
		yearly_visitor          AS yearly_visitor,
		zip         			AS zip
;
   
--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE generated_records
   INTO '${hive_database_name_gold}.${gold_opt_hit_data_tbl}' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_name=$suite_name, source_date=$source_date')
; 
				 
STORE union_error_records 
      INTO '${hive_database_name_incoming}.${incoming_error_hit_data_tbl}' 
	  USING org.apache.hive.hcatalog.pig.HCatStorer('suite_name=$suite_name,source_date=$source_date')
;

--##############################################################################
--#                              End                                           #
--##############################################################################			