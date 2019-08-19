--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Smith table from Gold table 
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
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
set hive.execution.engine=tez;

INSERT OVERWRITE DIRECTORY '${hivevar:location}' 
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
 SELECT
       EVAR24_ALERTS_NOTIFICATION as ALERTS_NOTIFICATION,   
 	   BROWSER_ID as BROWSER_ID          ,
       CAMPAIGN as CAMPAIGN ,         	  	
       EVAR42_CAMPAIGN_EVAR as CAMPAIGN_EVAR,           
       CONNECTION_TYPE_ID as CONNECTION_TYPE_ID,      
       CORP as CORP       ,
       COUNTRY_ID as COUNTRY_ID,              
       CUST as CUST      	  	,
       CUSTOM_LINK as CUSTOM_LINK,       	  	
       EVAR8 as CUSTOM_LINK_TRACKING,                  
       DWELLING_NBR as DWELLING_NBR   ,         
       EVENT_LIST as EVENT_LIST        ,    	
       PROP25_HULU_ID as HULU_ID        ,         
       ISP_ID as ISP_ID                 	,
       javascript_version_id as JAVASCRIPT_VERSION_ID,   
       language_id as LANGUAGE_ID            	  ,
       mobile_id as MOBILE_ID          	  	,
       monthly_visitor as MONTHLY_VISITOR     ,    
       operating_system_id as OPERATING_SYSTEM_ID,     
       opt_err_msg_id as OPT_ERR_MSG_ID         	,
       prop11_optimum_id as OPTIMUM_ID             ,	
       optimum_user_id as OPTIMUM_USER_ID         ,
       page_event as PAGE_EVENT              ,
       page_event_var1 as PAGE_EVENT_VAR1     ,    
       page_name_id as PAGE_NAME_ID            ,
       page_type as PAGE_TYPE          	  	,
       page_url as PAGE_URL                ,
       plugins as PLUGINS                 ,
       post_visid_high as POST_VISID_HIGH  ,       
       post_visid_low as POST_VISID_LOW     ,     
       referrer_type_id as REFERRER_TYPE_ID  ,      
       search_engine_id as SEARCH_ENGINE_ID   ,     
       site_section_id as SITE_SECTION_ID      , 	
       sourced_from_system as SOURCED_FROM_SYSTEM,     
       suite_id as SUITE_ID                ,
       evar26_tactic as TACTIC              , 	
       evar23_ttt_acct_number as TTT_ACCT_NUMBER,         
       visid_high as VISID_HIGH              ,
       visid_low as VISID_LOW            	,
       visit_date as VISIT_DATE            ,  
       visit_num as VISIT_NUM               ,
       visit_page_num as VISIT_PAGE_NUM      ,    
       visit_start_time_gmt as VISIT_START_TIME_GMT,    
       visid_timestamp as VISIT_TIMESTAMP         ,
       evar43_wildcard as WILDCARD 
   FROM  ${hivevar:hive_database_name_gold}.${hivevar:gold_opt_hit_data_esp_tbl}
   WHERE SOURCE_DATE='${hivevar:source_date}'
   ;
              