--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Loading data from incoming_hit_data_opt_app table to gold_opt_hit_data table
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
--#                                   Load                                     #
--##############################################################################

incoming_hit_data = 
	LOAD '${hive_database_name_incoming}.${incoming_hit_data_tbl}' 
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
incoming_search_engines = 
	LOAD '${hive_database_name_incoming}.${incoming_search_engines_tbl}' 
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
	
incoming_plugins_filter = 
	FILTER incoming_plugins 
	BY (suite_id=='$suite_id')
;
	
incoming_search_engines_filter = 
	FILTER incoming_search_engines 
	BY (suite_id=='$suite_id')
;
	
--##############################################################################
--#                                 Transform                                  #
--##############################################################################

	
--##############################################################################
--#         			Join with look-up                                      #
--##############################################################################
	
hit_data_records_join_browser = 
	JOIN incoming_hit_data_filter BY browser LEFT, 
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
											   
hit_data_records_join_search_engines = 
	JOIN hit_data_records_join_operating_systems BY search_engine LEFT,
	incoming_search_engines_filter BY search_engine_id
	USING 'replicated'
;

hit_data_record_join_plugins = 
   JOIN hit_data_records_join_search_engines BY  plugins LEFT,
   incoming_plugins_filter BY plugin_id
   USING 'replicated'
;   
  
generated_records = 
	FOREACH hit_data_record_join_plugins 
	GENERATE
        accept_language                       as       accept_language             ,    
		browser                              as       browser_id                  ,    
		browser_name                          as       browser_name                ,    
		channel                               as       channel                     ,    
		click_action                          as       click_action                ,    
		click_action_type                     as       click_action_type           ,    
		click_context                         as       click_context               ,    
		click_context_type                    as       click_context_type          ,    
		click_sourceid                        as       click_sourceid              ,    
		click_tag                             as       click_tag                   ,    
		color                                 as       color                       ,    
		hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::incoming_connection_type_filter::connection_type   as    connection_type   ,    
		hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::hit_data_records_join_connection_type::hit_data_records_join_browser::incoming_hit_data_filter::connection_type                       as       connection_type_id   ,    
		hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::hit_data_records_join_country::incoming_country_filter::country_name    as       country  ,    
		country                               as       country_id                  ,    
		daily_visitor                         as       daily_visitor               ,    
		date_time                             as       date_time                   ,    
		domain                                as       domain                      ,    
		duplicate_purchase                    as       duplicate_purchase          ,    
		evar1                                 as       evar1                       ,    
		evar10                                as       evar10                      ,    
		evar11                                as       evar11                      ,    
		evar12                                as       evar12                      ,    
		evar13                                as       evar13                      ,    
		evar14                                as       evar14                      ,    
		evar15                                as       evar15                      ,    
		evar16                                as       evar16                      ,    
		evar17                                as       evar17                      ,    
		evar18                                as       evar18                      ,    
		evar19                                as       evar19                      ,    
		evar2                                 as       evar2                       ,    
		evar20                                as       evar20                      ,    
		evar21                                as       evar21                      ,    
		evar22                                as       evar22                      ,    
		evar23                                as       evar23                      ,    
		evar24                                as       evar24                      ,    
		evar25                                as       evar25                      ,    
		evar26                                as       evar26                      ,    
		evar27                                as       evar27                      ,    
		evar28                                as       evar28                      ,    
		evar29                                as       evar29                      ,    
		evar3                                 as       evar3                       ,    
		evar30                                as       evar30                      ,    
		evar31                                as       evar31                      ,    
		evar32                                as       evar32                      ,    
		evar33                                as       evar33                      ,    
		evar34                                as       evar34                      ,    
		evar35                                as       evar35                      ,    
		evar36                                as       evar36                      ,    
		evar37                                as       evar37                      ,    
		evar38                                as       evar38                      ,    
		evar39                                as       evar39                      ,    
		evar4                                 as       evar4                       ,    
		evar40                                as       evar40                      ,    
		evar41                                as       evar41                      ,    
		evar42                                as       evar42                      ,    
		evar43                                as       evar43                      ,    
		evar44                                as       evar44                      ,    
		evar45                                as       evar45                      ,    
		evar46                                as       evar46                      ,    
		evar47                                as       evar47                      ,    
		evar48                                as       evar48                      ,    
		evar49                                as       evar49                      ,    
		evar5                                 as       evar5                       ,    
		evar50                                as       evar50                      ,    
		evar6                                 as       evar6                       ,    
		evar7                                 as       evar7                       ,    
		evar8                                 as       evar8                       ,    
		evar9                                 as       evar9                       ,    
		event_list                            as       event_list                  ,    
		exclude_hit                           as       exclude_hit                 ,    
		first_hit_page_url                    as       first_hit_page_url          ,    
		first_hit_pagename                    as       first_hit_pagename          ,    
		first_hit_referrer                    as       first_hit_referrer          ,    
		first_hit_time_gmt                    as       first_hit_time_gmt          ,    
		geo_city                              as       geo_city                    ,    
		geo_country                           as       geo_country                 ,    
		geo_region                            as       geo_region                  ,    
		hier1                                 as       hier1                       ,    
		hier2                                 as       hier2                       ,    
		hier3                                 as       hier3                       ,    
		hier4                                 as       hier4                       ,    
		hier5                                 as       hier5                       ,    
		hit_time_gmt                          as       hit_time_gmt                ,    
		homepage                              as       homepage                    ,    
		hourly_visitor                        as       hourly_visitor              ,    
		ip                                    as       ip                          ,    
		javascript_version                    as       javascript_version          ,    
		hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::hit_data_records_join_javascript_version::incoming_javascript_version_filter::javascript_version_id   as       javascript_id   ,    
		language                              as       language_id                 ,    
		hit_data_records_join_search_engines::hit_data_records_join_operating_systems::hit_data_records_join_languages::incoming_languages_filter::language_name  as       language_name               ,    
		last_hit_time_gmt                     as       last_hit_time_gmt           ,    
		last_purchase_num                     as       last_purchase_num           ,    
		last_purchase_time_gmt                as       last_purchase_time_gmt      ,    
		monthly_visitor                       as       monthly_visitor             ,    
		new_visit                             as       new_visit                   ,    
		os                                    as       operating_system_id         ,    
		hit_data_records_join_search_engines::hit_data_records_join_operating_systems::incoming_operating_systems_filter::operating_system_name     as       operating_system_name       ,    
		page_event                            as       page_event                  ,    
		page_event_var1                       as       page_event_var1             ,    
		page_event_var2                       as       page_event_var2             ,    
		page_type                             as       page_type                   ,    
		page_url                              as       page_url                    ,    
		pagename                              as       pagename                    ,    
		incoming_plugins_filter::plugin_name  as       plugin_name                 ,    
		plugins                               as       plugins                     ,    
		post_browser_height                   as       post_browser_height         ,    
		post_browser_width                    as       post_browser_width          ,    
		post_campaign                         as       post_campaign               ,    
		post_cookies                          as       post_cookies                ,    
		post_evar1                            as       post_evar1                  ,    
		post_evar10                           as       post_evar10                 ,    
		post_evar11                           as       post_evar11                 ,    
		post_evar12                           as       post_evar12                 ,    
		post_evar13                           as       post_evar13                 ,    
		post_evar14                           as       post_evar14                 ,    
		post_evar15                           as       post_evar15                 ,    
		post_evar16                           as       post_evar16                 ,    
		post_evar17                           as       post_evar17                 ,    
		post_evar18                           as       post_evar18                 ,    
		post_evar19                           as       post_evar19                 ,    
		post_evar2                            as       post_evar2                  ,    
		post_evar20                           as       post_evar20                 ,    
		post_evar21                           as       post_evar21                 ,    
		post_evar22                           as       post_evar22                 ,    
		post_evar23                           as       post_evar23                 ,    
		post_evar24                           as       post_evar24                 ,    
		post_evar25                           as       post_evar25                 ,    
		post_evar26                           as       post_evar26                 ,    
		post_evar27                           as       post_evar27                 ,    
		post_evar28                           as       post_evar28                 ,    
		post_evar29                           as       post_evar29                 ,    
		post_evar3                            as       post_evar3                  ,    
		post_evar30                           as       post_evar30                 ,    
		post_evar31                           as       post_evar31                 ,    
		post_evar32                           as       post_evar32                 ,    
		post_evar33                           as       post_evar33                 ,    
		post_evar34                           as       post_evar34                 ,    
		post_evar35                           as       post_evar35                 ,    
		post_evar36                           as       post_evar36                 ,    
		post_evar37                           as       post_evar37                 ,    
		post_evar38                           as       post_evar38                 ,    
		post_evar39                           as       post_evar39                 ,    
		post_evar4                            as       post_evar4                  ,    
		post_evar40                           as       post_evar40                 ,    
		post_evar41                           as       post_evar41                 ,    
		post_evar42                           as       post_evar42                 ,    
		post_evar43                           as       post_evar43                 ,    
		post_evar44                           as       post_evar44                 ,    
		post_evar45                           as       post_evar45                 ,    
		post_evar46                           as       post_evar46                 ,    
		post_evar47                           as       post_evar47                 ,    
		post_evar48                           as       post_evar48                 ,    
		post_evar49                           as       post_evar49                 ,    
		post_evar5                            as       post_evar5                  ,    
		post_evar50                           as       post_evar50                 ,    
		post_evar6                            as       post_evar6                  ,    
		post_evar7                            as       post_evar7                  ,    
		post_evar8                            as       post_evar8                  ,    
		post_evar9                            as       post_evar9                  ,    
		post_java_enabled                     as       post_java_enabled           ,    
		post_persistent_cookie                as       post_persistent_cookie      ,    
		post_t_time_info                      as       post_t_time_info            ,    
		prev_page                             as       prev_page                   ,    
		product_list                          as       product_list                ,    
		prop1                                 as       prop1                       ,    
		prop10                                as       prop10                      ,    
		prop11                                as       prop11                      ,    
		prop12                                as       prop12                      ,    
		prop13                                as       prop13                      ,    
		prop14                                as       prop14                      ,    
		prop15                                as       prop15                      ,    
		prop16                                as       prop16                      ,    
		prop17                                as       prop17                      ,    
		prop18                                as       prop18                      ,    
		prop19                                as       prop19                      ,    
		prop2                                 as       prop2                       ,    
		prop20                                as       prop20                      ,    
		prop21                                as       prop21                      ,    
		prop22                                as       prop22                      ,    
		prop23                                as       prop23                      ,    
		prop24                                as       prop24                      ,    
		prop25                                as       prop25                      ,    
		prop26                                as       prop26                      ,    
		prop27                                as       prop27                      ,    
		prop28                                as       prop28                      ,    
		prop29                                as       prop29                      ,    
		prop3                                 as       prop3                       ,    
		prop30                                as       prop30                      ,    
		prop31                                as       prop31                      ,    
		prop32                                as       prop32                      ,    
		prop33                                as       prop33                      ,    
		prop34                                as       prop34                      ,    
		prop35                                as       prop35                      ,    
		prop36                                as       prop36                      ,    
		prop37                                as       prop37                      ,    
		prop38                                as       prop38                      ,    
		prop39                                as       prop39                      ,    
		prop4                                 as       prop4                       ,    
		prop40                                as       prop40                      ,    
		prop41                                as       prop41                      ,    
		prop42                                as       prop42                      ,    
		prop43                                as       prop43                      ,    
		prop44                                as       prop44                      ,    
		prop45                                as       prop45                      ,    
		prop46                                as       prop46                      ,    
		prop47                                as       prop47                      ,    
		prop48                                as       prop48                      ,    
		prop49                                as       prop49                      ,    
		prop5                                 as       prop5                       ,    
		prop50                                as       prop50                      ,    
		prop6                                 as       prop6                       ,    
		prop7                                 as       prop7                       ,    
		prop8                                 as       prop8                       ,    
		prop9                                 as       prop9                       ,    
		purchaseid                            as       purchaseid                  ,    
		referrer                              as       referrer                    ,    
		resolution                            as       resolution                  ,    
		search_engine                         as       search_engine_id            ,    
		hit_data_records_join_search_engines::incoming_search_engines_filter::search_engine_name     as       search_engine_name          ,    
		service                               as       service                     ,    
		state                                 as       state                       ,    
		user_agent                            as       user_agent                  ,    
		user_server                           as       user_server                 ,    
		visid_high                            as       visid_high                  ,    
		visid_low                             as       visid_low                   ,    
		visit_num                             as       visit_num                   ,    
		visit_page_num                        as       visit_page_num              ,    
		visit_referrer                        as       visit_referrer              ,    
		visit_search_engine                   as       visit_search_engine         ,    
		visit_start_page_url                  as       visit_start_page_url        ,    
		visit_start_pagename                  as       visit_start_pagename        ,    
		visit_start_time_gmt                  as       visit_start_time_gmt        ,    
		yearly_visitor                        as       yearly_visitor              ,    
		zip                                   as       zip                             
;
   
--##############################################################################
--#                                 Store                                      #
--##############################################################################

STORE generated_records
   INTO '${hive_database_name_gold}.${gold_opt_hit_data_tbl}' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_name=$suite_name, source_date=$source_date')
; 
				 
--##############################################################################
--#                              End                                           #
--##############################################################################			