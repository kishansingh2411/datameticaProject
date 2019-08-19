--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Smith MKTG unified optimum usage table 
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
	BROWSER_ID                 	  DECIMAL(38,0) COMMENT '',
	CAMPAIGN                   	  VARCHAR(255) COMMENT '',
	CAMPAIGN_STACKING     	  	  VARCHAR(255) COMMENT '',
	CHECKOUT_ID          	      VARCHAR(255) COMMENT '', 
	CHECKOUT_FORM_FIELD           VARCHAR(255) COMMENT '',
	CONNECTION_TYPE_ID         	  DECIMAL(38,0) COMMENT '',
	COUNTRY_ID                 	  DECIMAL(38,0) COMMENT '',
	CUSTOM_LINK  				  VARCHAR(255) COMMENT '', 
	EVENT_LIST                 	  VARCHAR(500) COMMENT '',
	EXTERNAL_CAMPAIGN    	  	  VARCHAR(255) COMMENT '',
	FORM_ID         	          VARCHAR(255) COMMENT '',
	ISP_ID                     	  DECIMAL(38,0) COMMENT '',
	JAVASCRIPT_VERSION_ID      	  DECIMAL(38,0) COMMENT '',
	LANGUAGE_ID                	  DECIMAL(38,0) COMMENT '',
	LEAD_FORM_ID               	  VARCHAR(255) COMMENT '', 
	LEAD_FORM_FIELD	              VARCHAR(255) COMMENT '', 
	LIVE_CHAT_CLICK               VARCHAR(255) COMMENT '', 
	LIVE_CALL               	  VARCHAR(255) COMMENT '', 
	MOBILE_ID					  DECIMAL(38,0) COMMENT '',
	OPERATING_SYSTEM_ID           DECIMAL(38,0) COMMENT '',
	OPTIMUM_LINK			      VARCHAR(256) COMMENT '', 
	OPT_ERR_MSG_ID    			  DECIMAL(38,0) COMMENT '',
	ORDER_FORM_FIELD              VARCHAR(255) COMMENT '', 
	ORDER_FLOW_ID                 VARCHAR(255) COMMENT '', 
	PAGE_NAME_ID                  DECIMAL(38,0) COMMENT '', 
	PAGE_TYPE 						VARCHAR(255) COMMENT '',
	PAGE_URL                      VARCHAR(500) COMMENT '', 
	PLUGINS                       VARCHAR(255) COMMENT '', 
	POST_VISID_HIGH               VARCHAR(20) COMMENT '', 
	POST_VISID_LOW                VARCHAR(20) COMMENT '', 
	PRODUCT_LIST     			  VARCHAR(255) COMMENT '', 
	PURCHASE_ID     			  VARCHAR(255) COMMENT '', 
	REFERRER_TYPE_ID              DECIMAL(38,0) COMMENT '',
	SEARCH_ENGINE_ID              DECIMAL(38,0) COMMENT '',
	SITE_LANGUAGE                 VARCHAR(255) COMMENT '',
	SITE_SECTION_ID               DECIMAL(38,0) COMMENT '',
	SOURCED_FROM_SYSTEM    		  VARCHAR(60) COMMENT '', 
	SUITE_ID                      DECIMAL(38,0) COMMENT '',
	TRANSACTION_ID   		      VARCHAR(255) COMMENT '', 
	VISID_HIGH                	  VARCHAR(20) COMMENT '', 
	VISID_LOW                     VARCHAR(20) COMMENT '', 
	VISIT_DATE                	  TIMESTAMP COMMENT '', 
	VISIT_NUM                     DECIMAL(38,0) COMMENT '',
	VISIT_PAGE_NUM                DECIMAL(38,0) COMMENT '',
	VISID_TIMESTAMP               DECIMAL(38,0) COMMENT '', 
	VISITOR_TYPE  		          VARCHAR(255) COMMENT '' ,
	SITE_TYPE                    VARCHAR(255) COMMENT '',
	SITE_PAGE_TYPE              VARCHAR(255) COMMENT ''
)
PARTITIONED BY (SOURCE_DATE STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
LOCATION '${hivevar:location}'
;
--##############################################################################
--#                                    End                                     #
--##############################################################################