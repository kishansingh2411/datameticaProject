--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Smith onet_prod_esp_optimum_usage table 
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${job_name}.log
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

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
	ALERTS_NOTIFICATION          VARCHAR(255) 	COMMENT '',
   BROWSER_ID             	 decimal(38,0) 		COMMENT '',
   CAMPAIGN         	  	VARCHAR(38) 	COMMENT '',
   CAMPAIGN_EVAR             	VARCHAR(255) 	COMMENT '',
   CONNECTION_TYPE_ID           DECIMAL(38,0)	COMMENT '',
   CORP                    	BIGINT		COMMENT '',
   COUNTRY_ID                   DECIMAL(38,0)	COMMENT '',
   CUST               	  	VARCHAR(2)	COMMENT '',
   CUSTOM_LINK       	  	VARCHAR(255) 	COMMENT '',
   CUSTOM_LINK_TRACKING         VARCHAR(255) 	COMMENT '',  
   DWELLING_NBR                 VARCHAR(6)	COMMENT '',
   EVENT_LIST            	VARCHAR(500) 	COMMENT '',
   HULU_ID                     	VARCHAR(255) 	COMMENT '',
   ISP_ID                 	 DECIMAL(38,0)		COMMENT '',  
   JAVASCRIPT_VERSION_ID        DECIMAL(38,0) 	COMMENT '',
   LANGUAGE_ID            	DECIMAL(38,0)	COMMENT '',
   MOBILE_ID          	  	DECIMAL(38,0) 	COMMENT '',
   MONTHLY_VISITOR              DECIMAL(38,0) 	COMMENT '',
   OPERATING_SYSTEM_ID         	DECIMAL(38,0) 	COMMENT '',  
   OPT_ERR_MSG_ID         	 DECIMAL(38,0)		COMMENT '',
   OPTIMUM_ID             	VARCHAR(256)	COMMENT '',
   OPTIMUM_USER_ID            	INT	 	COMMENT '',
   PAGE_EVENT                   DECIMAL(38,0) 	COMMENT '',
   PAGE_EVENT_VAR1              VARCHAR(255) 	COMMENT '',
   PAGE_NAME_ID                 DECIMAL(38,0) 	COMMENT '',
   PAGE_TYPE          	  	VARCHAR(255) 	COMMENT '',
   PAGE_URL                   	VARCHAR(500)	COMMENT '',
   PLUGINS                    	VARCHAR(255) 	COMMENT '',
   POST_VISID_HIGH              VARCHAR(20) 	COMMENT '',
   POST_VISID_LOW               VARCHAR(20) 	COMMENT '',
   REFERRER_TYPE_ID             DECIMAL(38,0) 	COMMENT '',
   SEARCH_ENGINE_ID             DECIMAL(38,0)	COMMENT '',  
   SITE_SECTION_ID       	DECIMAL(38,0)		COMMENT '',
   SOURCED_FROM_SYSTEM          VARCHAR(60)	COMMENT '',
   SUITE_ID                   	INT       	COMMENT '',
   TACTIC               	VARCHAR(10) 	COMMENT '',
   TTT_ACCT_NUMBER              VARCHAR(255) 	COMMENT '',
   VISID_HIGH               	VARCHAR(20) 	COMMENT '',
   VISID_LOW            	VARCHAR(20) 	COMMENT '',
   VISIT_DATE                 	DATE        	COMMENT '',
   VISIT_NUM                	DECIMAL(38,0)	COMMENT '',
   VISIT_PAGE_NUM               DECIMAL(38,0) 	COMMENT '',
   VISIT_START_TIME_GMT         DECIMAL(38,0) 	COMMENT '',
   VISIT_TIMESTAMP              TIMESTAMP 	COMMENT '',
   WILDCARD                     VARCHAR(255) 	COMMENT ''    
)
PARTITIONED BY (${hivevar:partition_column} STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
LOCATION '${hivevar:location}'
;
--##############################################################################
--#                                    End                                     #
--##############################################################################