--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Deriving Smith KOM Unified Optimum Usage table from gold layer 
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

INSERT OVERWRITE TABLE ${hivevar:hive_database_name_smith}.${hivevar:smith_optimum_usage_table} 
PARTITION (source_date='${hivevar:source_date}')
 SELECT
    BROWSER_ID as BROWSER_ID ,                	 
	CAMPAIGN as CAMPAIGN  ,              
	EVAR3_CAMPAIGN_STACKING as CAMPAIGN_STACKING ,
	EVAR33_CHECKOUT_ID as CHECKOUT_ID ,
	PROP43_CHECKOUT_FORM_FIELD as CHECKOUT_FORM_FIELD    ,
	CONNECTION_TYPE_ID as CONNECTION_TYPE_ID  ,
	COUNTRY_ID as COUNTRY_ID   ,
	PROP10_CUSTOM_LINK as CUSTOM_LINK  ,               
	EVENT_LIST as EVENT_LIST,               
	EVAR26_EXTERNAL_CAMPAIGN as EXTERNAL_CAMPAIGN ,   	 
	EVAR22_FORM_ID as FORM_ID ,
	ISP_ID as ISP_ID  ,       
	JAVASCRIPT_VERSION_ID  as JAVASCRIPT_VERSION_ID ,   
	LANGUAGE_ID as LANGUAGE_ID    ,          
	EVAR31_LEAD_FORM_ID as LEAD_FORM_ID    ,         
	PROP41_LEAD_FORM_FIELD as LEAD_FORM_FIELD	 ,  
	EVAR12_LIVE_CHAT_CLICK as LIVE_CHAT_CLICK ,
	EVAR28_LIVE_CALL as LIVE_CALL ,
	MOBILE_ID as MOBILE_ID,	
	OPERATING_SYSTEM_ID as OPERATING_SYSTEM_ID,      
	EVAR10_OPTIMUM_LINK as OPTIMUM_LINK,
	OPT_ERR_MSG_ID as OPT_ERR_MSG_ID  ,
	PROP42_ORDER_FORM_FIELD as ORDER_FORM_FIELD ,        
	EVAR32_ORDER_FLOW_ID as ORDER_FLOW_ID  ,
	PAGE_NAME_ID      as PAGE_NAME_ID,  
	EVAR2_PAGE_TYPE as PAGE_TYPE ,      
	PAGE_URL as PAGE_URL ,            
	PLUGINS as PLUGINS ,             
	POST_VISID_HIGH as POST_VISID_HIGH,          
	POST_VISID_LOW   as POST_VISID_LOW  ,      
	PRODUCT_LIST as PRODUCT_LIST,     		
	EVAR29_PURCHASEID as PURCHASE_ID ,		
	REFERRER_TYPE_ID as REFERRER_TYPE_ID ,       
	SEARCH_ENGINE_ID as SEARCH_ENGINE_ID,  
	EVAR19_SITE_LANGUAGE as SITE_LANGUAGE,      
	SITE_SECTION_ID as SITE_SECTION_ID  ,        
	SOURCED_FROM_SYSTEM as SOURCED_FROM_SYSTEM    	,
	SUITE_ID as SUITE_ID,
	EVAR36_TRANSACTIONID as TRANSACTION_ID 	,
	VISID_HIGH as VISID_HIGH, 
	VISID_LOW as VISID_LOW,                
	VISIT_DATE as VISIT_DATE,   
	VISIT_NUM  as VISIT_NUM,
	VISIT_PAGE_NUM  as VISIT_PAGE_NUM,	  
	VISID_TIMESTAMP as VISID_TIMESTAMP ,    
	EVAR8_VISITOR_TYPE as VISITOR_TYPE ,
	SITE_TYPE   as SITE_TYPE,
	PROP45_PAGE_TYPE as  SITE_PAGE_TYPE          
FROM  ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_table}
WHERE SOURCE_DATE='${hivevar:source_date}'
;
