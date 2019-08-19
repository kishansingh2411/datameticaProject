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
define resolvePluginId com.cablevision.edh.udf.ResolvePluginId();

incoming_hit_data = 
	LOAD '${hive_database_name_incoming}.${incoming_hit_data_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;
	
gold_cust_services_last_rec = 
	LOAD '${hive_database_name_gold_ods}.${gold_cust_services_last_rec}' 
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
	
--incoming_referrer_type = 
--	LOAD '${hive_database_name_incoming}.${incoming_referrer_type_tbl}' 
--	USING org.apache.hive.hcatalog.pig.HCatLoader()
--;
	
incoming_search_engines = 
	LOAD '${hive_database_name_incoming}.${incoming_search_engines_tbl}' 
	USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_plugins = 
	LOAD '${hive_database_name_incoming}.${incoming_plugins_tbl}' 
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

--##############################################################################
--#                                 Filter                                     #
--##############################################################################

incoming_hit_data_filter = 
	FILTER incoming_hit_data 
    BY (suite_name=='$suite_name' AND source_date == '$source_date' )
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
	
--incoming_referrer_type_filter = 
--	FILTER incoming_referrer_type 
--	BY (suite_id=='$suite_id')
--;
	
incoming_search_engines_filter = 
	FILTER incoming_search_engines 
	BY (suite_id=='$suite_id')
;

incoming_plugins_filter = 
	FILTER incoming_plugins 
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

--##############################################################################
--#                                 Transform  and Join                        #
--##############################################################################

join_incoming_hit_data_stock36 = 
	JOIN incoming_hit_data_filter by home_id LEFT,
	gold_cust_services_last_rec by home_id;
                              
hit_data_records_join_browser = 
	JOIN join_incoming_hit_data_stock36 BY browser LEFT, 
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
						
--hit_data_records_join_referrer_type = 
--	JOIN hit_data_records_join_operating_systems BY ref_type LEFT,
--    incoming_referrer_type_filter BY referrer_type_id
--   USING 'replicated'
--;	
						   
hit_data_records_join_search_engines = 
--	JOIN hit_data_records_join_referrer_type BY search_engine LEFT,
	JOIN hit_data_records_join_operating_systems BY search_engine LEFT,
	incoming_search_engines_filter BY search_engine_id
	USING 'replicated'
;
 
 hit_data_records_join_isp = 
	JOIN hit_data_records_join_search_engines BY domain LEFT,
	incoming_optimum_isp_filter BY isp_name
	USING 'replicated'
;
	
hit_data_records_join_pagenames = 
	JOIN hit_data_records_join_isp BY evar1 LEFT,
    incoming_optimum_pagenames_filter BY pagename
    USING 'replicated'
;
		
hit_data_records_join_site_section = 
	JOIN hit_data_records_join_pagenames BY channel LEFT,
	incoming_optimum_site_section_filter BY site_section_name
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
   FOREACH hit_data_records_join_site_section 
   GENERATE ..os,
	  resolvePluginId(plugins, incoming_plugins_generate.plugin_rec_bag) AS plugin_name,
	  plugins..
;
    
                              
join_incoming_hit_data_stock36_filter =
	FOREACH hit_data_records_join_plugins
	GENERATE ..incoming_hit_data_filter::prop1,
		(TRIM(incoming_hit_data_filter::home_id) == '' OR incoming_hit_data_filter::home_id is null ? '-1' : incoming_hit_data_filter::mac_id) as mac_id,
		incoming_hit_data_filter::prop2 .. incoming_hit_data_filter::prop10,
		(TRIM(incoming_hit_data_filter::home_id) == '' OR incoming_hit_data_filter::home_id is null ? '-1' : incoming_hit_data_filter::home_id) as home_id,
		incoming_hit_data_filter::prop11 .. incoming_hit_data_filter::source_date,
		(TRIM(incoming_hit_data_filter::home_id) == '' OR incoming_hit_data_filter::home_id is null ?  -1  : gold_cust_services_last_rec::id_service_rec) as id_service_rec,
		(TRIM(incoming_hit_data_filter::home_id) == '' OR incoming_hit_data_filter::home_id is null ? '-1' : gold_cust_services_last_rec::account_number) as account_number,
		gold_cust_services_last_rec::id_service_type..
;

generated_records = 
	FOREACH join_incoming_hit_data_stock36_filter
	GENERATE	accept_language as accept_language,
		(TRIM(account_number) == '' OR account_number is null ? '-1' : account_number ) as account_number,
		browser                 AS browser_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::hit_data_records_join_browser::incoming_browser_filter::browser_name AS browser_name,
		channel as channel,
		click_action as click_action,
		click_action_type as click_action_type,
		click_context as click_context,
		click_context_type as click_context_type,
		click_sourceid as click_sourceid,
		click_tag as click_tag,
		color as color,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::incoming_connection_type_filter::connection_type AS connection_type,
	    hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::incoming_connection_type_filter::connection_type_id AS connection_type_id,
		country                 AS country_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::incoming_country_filter::country_name AS country_name,
		daily_visitor as daily_visitor,
		date_time as date_time,
		domain as domain,
		duplicate_purchase as duplicate_purchase,
		evar1 as evar1,
		evar10 as evar10,
		evar11 as evar11,
		evar12 as evar12,
		evar13 as evar13,
		evar14 as evar14,
		evar15 as evar15,
		evar16 as evar16,
		evar17 as evar17,
		evar18 as evar18,
		evar19 as evar19,
		evar2 as evar2,
		evar20 as evar20,
		evar21 as evar21,
		evar22 as evar22,
		evar23 as evar23,
		evar24 as evar24,
		evar25 as evar25,
		evar26 as evar26,
		evar27 as evar27,
		evar28 as evar28,
		evar29 as evar29,
		evar3 as evar3,
		evar30 as evar30,
		evar31 as evar31,
		evar32 as evar32,
		evar33 as evar33,
		evar34 as evar34,
		evar35 as evar35,
		evar36 as evar36,
		evar37 as evar37,
		evar38 as evar38,
		evar39 as evar39,
		evar4 as evar4,
		evar40 as evar40,
		evar41 as evar41,
		evar42 as evar42,
		evar43 as evar43,
		evar44 as evar44,
		evar45 as evar45,
		evar46 as evar46,
		evar47 as evar47,
		evar48 as evar48,
		evar49 as evar49,
		evar5 as evar5,
		evar50 as evar50,
		evar6 as evar6,
		evar7 as evar7,
		evar8 as evar8,
		evar9 as evar9,
		event_list as event_list,
		exclude_hit as exclude_hit,
		first_hit_page_url as first_hit_page_url,
		first_hit_pagename as first_hit_pagename,
		first_hit_referrer as first_hit_referrer,
		first_hit_time_gmt as first_hit_time_gmt,
		geo_city as geo_city,
		geo_country as geo_country,
		geo_region as geo_region,
		hier1 as hier1,
		hier2 as hier2,
		hier3 as hier3,
		hier4 as hier4,
		hier5 as hier5,
		hit_time_gmt as hit_time_gmt,
		home_id as home_id,
		homepage as homepage,
		hourly_visitor as hourly_visitor,
		id_service_rec as id_service_rec,
		ip as ip,
		hit_data_records_join_pagenames::hit_data_records_join_isp::incoming_optimum_isp_filter::isp_id AS isp_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::incoming_optimum_isp_filter::isp_name AS isp_name,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::incoming_javascript_version_filter::javascript_version AS javascript_version,
		javascript              AS javascript_version_id,		
		language                AS language_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::incoming_languages_filter::language_name AS language_name,
		last_hit_time_gmt as last_hit_time_gmt,
		last_purchase_num as last_purchase_num,
		last_purchase_time_gmt as last_purchase_time_gmt,
		mac_id as mac_id,
		monthly_visitor as monthly_visitor,
		new_visit as new_visit,
		os      AS operating_system_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::incoming_operating_systems_filter::operating_system_name AS operating_system_name,
		page_event as page_event,
		page_event_var1 as page_event_var1,
		page_event_var2 as page_event_var2,
		hit_data_records_join_pagenames::incoming_optimum_pagenames_filter::page_name_id AS page_name_id,
		page_type         		AS page_type,          	
		page_url         		AS page_url,       
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::hit_data_records_join_browser::join_incoming_hit_data_stock36::incoming_hit_data_filter::pagename AS pagename,
		plugin_name             AS plugin_name,
		plugins         		AS plugins,      
		post_browser_height as post_browser_height,
		post_browser_width as post_browser_width,
		post_campaign as post_campaign,
		post_cookies as post_cookies,
		post_evar1 as post_evar1,
		post_evar10 as post_evar10,
		post_evar11 as post_evar11,
		post_evar12 as post_evar12,
		post_evar13 as post_evar13,
		post_evar14 as post_evar14,
		post_evar15 as post_evar15,
		post_evar16 as post_evar16,
		post_evar17 as post_evar17,
		post_evar18 as post_evar18,
		post_evar19 as post_evar19,
		post_evar2 as post_evar2,
		post_evar20 as post_evar20,
		post_evar21 as post_evar21,
		post_evar22 as post_evar22,
		post_evar23 as post_evar23,
		post_evar24 as post_evar24,
		post_evar25 as post_evar25,
		post_evar26 as post_evar26,
		post_evar27 as post_evar27,
		post_evar28 as post_evar28,
		post_evar29 as post_evar29,
		post_evar3 as post_evar3,
		post_evar30 as post_evar30,
		post_evar31 as post_evar31,
		post_evar32 as post_evar32,
		post_evar33 as post_evar33,
		post_evar34 as post_evar34,
		post_evar35 as post_evar35,
		post_evar36 as post_evar36,
		post_evar37 as post_evar37,
		post_evar38 as post_evar38,
		post_evar39 as post_evar39,
		post_evar4 as post_evar4,
		post_evar40 as post_evar40,
		post_evar41 as post_evar41,
		post_evar42 as post_evar42,
		post_evar43 as post_evar43,
		post_evar44 as post_evar44,
		post_evar45 as post_evar45,
		post_evar46 as post_evar46,
		post_evar47 as post_evar47,
		post_evar48 as post_evar48,
		post_evar49 as post_evar49,
		post_evar5 as post_evar5,
		post_evar50 as post_evar50,
		post_evar6 as post_evar6,
		post_evar7 as post_evar7,
		post_evar8 as post_evar8,
		post_evar9 as post_evar9,
		post_java_enabled as post_java_enabled,
		post_persistent_cookie as post_persistent_cookie,
		post_t_time_info as post_t_time_info,
		prev_page as prev_page,
		product_list as product_list,
		prop12 as prop12,
		prop13 as prop13,
		prop14 AS prop14,
		prop15 AS prop15,
		prop16 as prop16,
		prop17 as prop17,
		prop18 as prop18,
		prop19 as prop19,
		prop2 as prop2,
		prop20 as prop20,
		prop21 as prop21,
		prop22 as prop22,
		prop23 as prop23,
		prop24 as prop24,
		prop25 as prop25,
		prop26 as prop26,
		prop27 as prop27,
		prop28 as prop28,
		prop29 as prop29,
		prop3 as prop3,
		prop30 as prop30,
		prop31 as prop31,
		prop32 as prop32,
		prop33 as prop33,
		prop34 as prop34,
		prop35 as prop35,
		prop36 as prop36,
		prop37 as prop37,
		prop38 as prop38,
		prop39 as prop39,
		prop4 as prop4,
		prop40 as prop40,
		prop41 as prop41,
		prop42 as prop42,
		prop43 as prop43,
		prop44 AS prop44,
		prop45 AS prop45,
		prop46 AS prop46,
		prop47 AS prop47,
		prop48 AS prop48,
		prop49 AS prop49,
		prop5 AS prop5,
		prop50 AS prop50,
		prop6 AS prop6,
		prop7 AS prop7,
		prop8 AS prop8,
		prop9 AS prop9,
		purchaseid AS purchaseid,
		referrer AS referrer,
		resolution AS resolution,
		search_engine AS search_engine_id,
		hit_data_records_join_pagenames::hit_data_records_join_isp::hit_data_records_join_search_engines::incoming_search_engines_filter::search_engine_name AS search_engine_name,
		service as service,
		incoming_optimum_site_section_filter::site_section_id AS site_section_id, 
		channel AS site_section_name,
		'omniture' AS sourced_from_system,
		state AS state,
		user_agent as user_agent,
		user_server AS user_server,
		visid_high AS visid_high,
		visid_low AS visid_low,
		visit_num AS visit_num,
		visit_page_num AS visit_page_num,
		visit_referrer AS visit_referrer,
		visit_search_engine AS visit_search_engine,
		visit_start_page_url AS visit_start_page_url,
		visit_start_pagename AS visit_start_pagename,
		visit_start_time_gmt AS visit_start_time_gmt,
		yearly_visitor AS yearly_visitor,
		zip AS zip
;                                                    
   
--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE generated_records
   INTO '${hive_database_name_gold}.${gold_stock36_hit_data_tbl}' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_name=$suite_name, source_date=$source_date')
; 
				 
--##############################################################################
--#                              End                                           #
--##############################################################################			