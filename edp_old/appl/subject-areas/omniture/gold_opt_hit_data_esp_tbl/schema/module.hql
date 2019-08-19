--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_opt_hit_data_esp table in Gold layer
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
   ACCEPT_LANGUAGE            	  VARCHAR(255) COMMENT '',
   BROWSER_HEIGHT             	  VARCHAR(38) COMMENT '',
   BROWSER_ID                 	  DECIMAL(38,0) COMMENT '',  
   BROWSER_NAME               	  VARCHAR(100) COMMENT 'NAME OF BROWSER',
   BROWSER_WIDTH              	  VARCHAR(38) COMMENT '',
   C_COLOR                    	  VARCHAR(255) COMMENT '',
   CAMPAIGN                   	  VARCHAR(255) COMMENT '',
   CLICK_ACTION               	  VARCHAR(255) COMMENT '',
   CLICK_ACTION_TYPE          	  VARCHAR(38) COMMENT '',
   CLICK_CONTEXT              	  VARCHAR(255) COMMENT '',
   CLICK_CONTEXT_TYPE         	  VARCHAR(38) COMMENT '',
   CLICK_SOURCEID             	  VARCHAR(38) COMMENT '',
   CLICK_TAG                  	  VARCHAR(255) COMMENT '',
   CODE_VER                   	  VARCHAR(255) COMMENT '',
   COLOR                      	  VARCHAR(38) COMMENT '',
   CONNECTION_TYPE            	  VARCHAR(100) COMMENT 'NAME OF CONNECTION TYPE',
   CONNECTION_TYPE_ID         	  DECIMAL(38,0) COMMENT '',  
   COOKIES                    	  CHAR(1) COMMENT '',
   CORP                       	  INT COMMENT '',
   COUNTRY_ID                 	  DECIMAL(38,0) COMMENT '',  
   COUNTRY_NAME               	  VARCHAR(100) COMMENT 'NAME OF THE COUNTRY',
   CT_CONNECT_TYPE            	  VARCHAR(255) COMMENT '',
   CURR_FACTOR                	  DECIMAL(38,0) COMMENT '',
   CURR_RATE                  	  DECIMAL(24,12) COMMENT '',
   CURRENCY                   	  VARCHAR(255) COMMENT '',
   CUST                       	  VARCHAR(10) COMMENT '',
   CUST_HIT_TIME_GMT          	  DECIMAL(38,0) COMMENT '',
   CUST_VISID                 	  VARCHAR(255) COMMENT '',
   CUSTOM_LINK                	  VARCHAR(255) COMMENT '',
   DAILY_VISITOR              	  DECIMAL(38,0) COMMENT '',
   DTM_CREATED                	  TIMESTAMP COMMENT '',  
   DUPLICATE_EVENTS           	  VARCHAR(255) COMMENT '',
   DUPLICATE_PURCHASE         	  DECIMAL(38,0) COMMENT '',
   DUPLICATED_FROM            	  VARCHAR(255) COMMENT '',
   DWELLING_NBR               	  VARCHAR(10) COMMENT '',
   EVAR1_PAGENAME             	  VARCHAR(255) COMMENT '',
   EVAR10                     	  VARCHAR(255) COMMENT '',
   EVAR11                     	  VARCHAR(255) COMMENT '',
   EVAR12                     	  VARCHAR(255) COMMENT '',
   EVAR13                     	  VARCHAR(255) COMMENT '',
   EVAR14                     	  VARCHAR(255) COMMENT '',
   EVAR15                     	  VARCHAR(255) COMMENT '',
   EVAR16                     	  VARCHAR(255) COMMENT '',
   EVAR17                     	  VARCHAR(255) COMMENT '',
   EVAR18                     	  VARCHAR(255) COMMENT '',
   EVAR19                     	  VARCHAR(255) COMMENT '',
   EVAR2                      	  VARCHAR(255) COMMENT '',
   EVAR20                     	  VARCHAR(255) COMMENT '',
   EVAR21                     	  VARCHAR(255) COMMENT '',
   EVAR22                     	  VARCHAR(255) COMMENT '',
   EVAR23_TTT_ACCT_NUMBER     	  VARCHAR(255) COMMENT '',
   EVAR24_ALERTS_NOTIFICATION 	  VARCHAR(255) COMMENT '',
   EVAR25                     	  VARCHAR(255) COMMENT '',
   EVAR26_TACTIC              	  VARCHAR(255) COMMENT '',
   EVAR27                     	  VARCHAR(255) COMMENT '',
   EVAR28                     	  VARCHAR(255) COMMENT '',
   EVAR29                     	  VARCHAR(255) COMMENT '',
   EVAR3                      	  VARCHAR(255) COMMENT '',
   EVAR30                     	  VARCHAR(255) COMMENT '',
   EVAR31                     	  VARCHAR(255) COMMENT '',
   EVAR32                     	  VARCHAR(255) COMMENT '',
   EVAR33                     	  VARCHAR(255) COMMENT '',
   EVAR34                     	  VARCHAR(255) COMMENT '',
   EVAR35                     	  VARCHAR(255) COMMENT '',
   EVAR36                     	  VARCHAR(255) COMMENT '',
   EVAR37                     	  VARCHAR(255) COMMENT '',
   EVAR38                     	  VARCHAR(255) COMMENT '',
   EVAR39                     	  VARCHAR(255) COMMENT '',
   EVAR4                      	  VARCHAR(255) COMMENT '',
   EVAR40                     	  VARCHAR(255) COMMENT '',
   EVAR41                     	  VARCHAR(255) COMMENT '',
   EVAR42_CAMPAIGN_EVAR       	  VARCHAR(255) COMMENT '',
   EVAR43_WILDCARD            	  VARCHAR(255) COMMENT '',
   EVAR44                     	  VARCHAR(255) COMMENT '',
   EVAR45                     	  VARCHAR(255) COMMENT '',
   EVAR46                     	  VARCHAR(255) COMMENT '',
   EVAR47                     	  VARCHAR(255) COMMENT '',
   EVAR48                     	  VARCHAR(255) COMMENT '',
   EVAR49                     	  VARCHAR(255) COMMENT '',
   EVAR5                      	  VARCHAR(255) COMMENT '',
   EVAR50                     	  VARCHAR(255) COMMENT '',
   EVAR51                     	  VARCHAR(255) COMMENT '',
   EVAR52                     	  VARCHAR(255) COMMENT '',
   EVAR53                     	  VARCHAR(255) COMMENT '',
   EVAR54                     	  VARCHAR(255) COMMENT '',
   EVAR55                     	  VARCHAR(255) COMMENT '',
   EVAR56                     	  VARCHAR(255) COMMENT '',
   EVAR57                     	  VARCHAR(255) COMMENT '',
   EVAR58                     	  VARCHAR(255) COMMENT '',
   EVAR59                     	  VARCHAR(255) COMMENT '',
   EVAR6                      	  VARCHAR(255) COMMENT '',
   EVAR60                     	  VARCHAR(255) COMMENT '',
   EVAR61                     	  VARCHAR(255) COMMENT '',
   EVAR62                     	  VARCHAR(255) COMMENT '',
   EVAR63                     	  VARCHAR(255) COMMENT '',
   EVAR64                     	  VARCHAR(255) COMMENT '',
   EVAR65                     	  VARCHAR(255) COMMENT '',
   EVAR66                     	  VARCHAR(255) COMMENT '',
   EVAR67                     	  VARCHAR(255) COMMENT '',
   EVAR68                     	  VARCHAR(255) COMMENT '',
   EVAR69                     	  VARCHAR(255) COMMENT '',
   EVAR7                      	  VARCHAR(255) COMMENT '',
   EVAR70                     	  VARCHAR(255) COMMENT '',
   EVAR71                     	  VARCHAR(255) COMMENT '',
   EVAR72                     	  VARCHAR(255) COMMENT '',
   EVAR73                     	  VARCHAR(255) COMMENT '',
   EVAR74                     	  VARCHAR(255) COMMENT '',
   EVAR75                     	  VARCHAR(255) COMMENT '',
   EVAR8                      	  VARCHAR(255) COMMENT '',
   EVAR9                      	  VARCHAR(255) COMMENT '',
   EVENT_LIST                 	  VARCHAR(255) COMMENT '',
   EXCLUDE_HIT                	  DECIMAL(38,0) COMMENT '',
   FIRST_HIT_PAGE_URL         	  VARCHAR(255) COMMENT '',
   FIRST_HIT_PAGENAME         	  VARCHAR(255) COMMENT '',
   FIRST_HIT_REFERRER         	  VARCHAR(255) COMMENT '',
   FIRST_HIT_TIME_GMT         	  DECIMAL(38,0) COMMENT '',
   GEO_CITY                   	  VARCHAR(255) COMMENT '',
   GEO_COUNTRY                	  VARCHAR(255) COMMENT '',
   GEO_DMA                    	  DECIMAL(38,0) COMMENT '',
   GEO_REGION                 	  VARCHAR(255) COMMENT '',
   GEO_ZIP                    	  VARCHAR(255) COMMENT '',
   HIER1                      	  VARCHAR(255) COMMENT '',
   HIER2                      	  VARCHAR(255) COMMENT '',
   HIER3                      	  VARCHAR(255) COMMENT '',
   HIER4                      	  VARCHAR(255) COMMENT '',
   HIER5                      	  VARCHAR(255) COMMENT '',
   HIT_SOURCE                 	  DECIMAL(38,0) COMMENT '',
   HIT_TIME_GMT               	  DECIMAL(38,0) COMMENT '',
   HITID_HIGH                 	  VARCHAR(255) COMMENT '',
   HITID_LOW                  	  VARCHAR(255) COMMENT '',
   HOMEPAGE                   	  CHAR(1) COMMENT '',
   HOURLY_VISITOR             	  DECIMAL(38,0) COMMENT '',   
   IP                         	  VARCHAR(255) COMMENT '',
   IP2                        	  VARCHAR(255) COMMENT '',
   ISP_ID                     	  DECIMAL(38,0) COMMENT 'Unique Isp_id assigned to isp_name',
   ISP_NAME                   	  VARCHAR(255) COMMENT 'Isp name derived from column Domain in incoming_hit_data',
   J_JSCRIPT                  	  VARCHAR(255) COMMENT '',
   JAVA_ENABLED               	  CHAR(1) COMMENT '',
   JAVASCRIPT_VERSION         	  VARCHAR(100) COMMENT 'NAME OF JAVASCRIPT VERSION',
   JAVASCRIPT_VERSION_ID      	  DECIMAL(38,0) COMMENT 'Unique id assigned to javascript version',  
   LANGUAGE_ID                	  DECIMAL(38,0) COMMENT ' Unique id assigned to languages',  
   LANGUAGE_NAME              	  VARCHAR(100) COMMENT 'NAME OF LANGUAGE',
   LAST_HIT_TIME_GMT          	  DECIMAL(38,0) COMMENT '',
   LAST_PURCHASE_NUM          	  DECIMAL(38,0) COMMENT '',
   LAST_PURCHASE_TIME_GMT     	  DECIMAL(38,0) COMMENT '',
   MOBILE_ID                  	  DECIMAL(10,0) COMMENT '',
   MONTHLY_VISITOR            	  DECIMAL(38,0) COMMENT '',
   MVVAR1                     	  VARCHAR(500) COMMENT '',
   MVVAR2                     	  VARCHAR(500) COMMENT '',
   MVVAR3                     	  VARCHAR(500) COMMENT '',
   NAMESPACE                  	  VARCHAR(500) COMMENT '',
   NEW_VISIT                  	  DECIMAL(38,0) COMMENT '',
   OPERATING_SYSTEM_ID        	  DECIMAL(38,0) COMMENT 'Unique id assigned to operating system',  
   OPERATING_SYSTEM_NAME      	  VARCHAR(100) COMMENT 'NAME OF OPERATING SYSTEM',   
   OPT_ERR_MSG_ID             	  DECIMAL(38,0) COMMENT 'Unique ids derived from incoming_hit_data',   
   OPTIMUM_USER_ID            	  INT COMMENT '',
   P_PLUGINS                  	  VARCHAR(4000) COMMENT '',
   PAGE_EVENT                 	  DECIMAL(38,0) COMMENT '',
   PAGE_EVENT_VAR1            	  VARCHAR(500) COMMENT '',
   PAGE_EVENT_VAR2            	  VARCHAR(255) COMMENT '',
   PAGE_EVENT_VAR3            	  VARCHAR(500) COMMENT '',
   PAGE_NAME_ID               	  DECIMAL(38,0) COMMENT '',
   PAGE_TYPE                  	  VARCHAR(255) COMMENT '',
   PAGE_URL                   	  VARCHAR(255) COMMENT '',
   PAGENAME                   	  VARCHAR(255) COMMENT '',
   PAID_SEARCH                	  DECIMAL(38,0) COMMENT '',
   PARTNER_PLUGINS            	  VARCHAR(500) COMMENT '',
   PERSISTENT_COOKIE          	  CHAR(1) COMMENT '',
   PLUGIN_NAME                	  VARCHAR(100) COMMENT 'PLUGIN NAME',
   PLUGINS                    	  VARCHAR(255) COMMENT '',
   POST_BROWSER_HEIGHT        	  DECIMAL(38,0) COMMENT '',
   POST_BROWSER_WIDTH         	  DECIMAL(38,0) COMMENT '',
   POST_CAMPAIGN              	  VARCHAR(255) COMMENT '',
   POST_CHANNEL               	  VARCHAR(255) COMMENT '',
   POST_COOKIES               	  CHAR(1) COMMENT '',
   POST_CURRENCY              	  VARCHAR(255) COMMENT '',
   POST_CUST_HIT_TIME_GMT     	  DECIMAL(38,0) COMMENT '',
   POST_CUST_VISID            	  DECIMAL(38,0) COMMENT '',
   POST_EVAR1                 	  VARCHAR(255) COMMENT '',
   POST_EVAR10                	  VARCHAR(255) COMMENT '',
   POST_EVAR11                	  VARCHAR(255) COMMENT '',
   POST_EVAR12                	  VARCHAR(255) COMMENT '',
   POST_EVAR13                	  VARCHAR(255) COMMENT '',
   POST_EVAR14                	  VARCHAR(255) COMMENT '',
   POST_EVAR15                	  VARCHAR(255) COMMENT '',
   POST_EVAR16                	  VARCHAR(255) COMMENT '',
   POST_EVAR17                	  VARCHAR(255) COMMENT '',
   POST_EVAR18                	  VARCHAR(255) COMMENT '',
   POST_EVAR19	                  VARCHAR(255) COMMENT '',
   POST_EVAR2                 	  VARCHAR(255) COMMENT '',
   POST_EVAR20	                  VARCHAR(255) COMMENT '',
   POST_EVAR21	                  VARCHAR(255) COMMENT '',
   POST_EVAR22	                  VARCHAR(255) COMMENT '',
   POST_EVAR23	                  VARCHAR(255) COMMENT '',
   POST_EVAR24	                  VARCHAR(255) COMMENT '',
   POST_EVAR25	                  VARCHAR(255) COMMENT '',
   POST_EVAR26	                  VARCHAR(255) COMMENT '',
   POST_EVAR27	                  VARCHAR(255) COMMENT '',
   POST_EVAR28	                  VARCHAR(255) COMMENT '',
   POST_EVAR29	                  VARCHAR(255) COMMENT '',
   POST_EVAR3                 	  VARCHAR(255) COMMENT '',
   POST_EVAR30	                  VARCHAR(255) COMMENT '',
   POST_EVAR31	                  VARCHAR(255) COMMENT '',
   POST_EVAR32	                  VARCHAR(255) COMMENT '',
   POST_EVAR33	                  VARCHAR(255) COMMENT '',
   POST_EVAR34	                  VARCHAR(255) COMMENT '',
   POST_EVAR35	                  VARCHAR(255) COMMENT '',
   POST_EVAR36	                  VARCHAR(255) COMMENT '',
   POST_EVAR37	                  VARCHAR(255) COMMENT '',
   POST_EVAR38	                  VARCHAR(255) COMMENT '',
   POST_EVAR39	                  VARCHAR(255) COMMENT '',
   POST_EVAR4                    VARCHAR(255) COMMENT '',
   POST_EVAR40	                  VARCHAR(255) COMMENT '',
   POST_EVAR41	                  VARCHAR(255) COMMENT '',
   POST_EVAR42	                  VARCHAR(255) COMMENT '',
   POST_EVAR43	                  VARCHAR(255) COMMENT '',
   POST_EVAR44	                  VARCHAR(255) COMMENT '',
   POST_EVAR45	                  VARCHAR(255) COMMENT '',
   POST_EVAR46	                  VARCHAR(255) COMMENT '',
   POST_EVAR47	                  VARCHAR(255) COMMENT '',
   POST_EVAR48	                  VARCHAR(255) COMMENT '',
   POST_EVAR49	                  VARCHAR(255) COMMENT '',
   POST_EVAR5                    VARCHAR(255) COMMENT '',
   POST_EVAR50	                  VARCHAR(255) COMMENT '',
   POST_EVAR51	                  VARCHAR(255) COMMENT '',
   POST_EVAR52	                  VARCHAR(255) COMMENT '',
   POST_EVAR53	                  VARCHAR(255) COMMENT '',
   POST_EVAR54	                  VARCHAR(255) COMMENT '',
   POST_EVAR55	                  VARCHAR(255) COMMENT '',
   POST_EVAR56	                  VARCHAR(255) COMMENT '',
   POST_EVAR57	                  VARCHAR(255) COMMENT '',
   POST_EVAR58	                  VARCHAR(255) COMMENT '',
   POST_EVAR59	                  VARCHAR(255) COMMENT '',
   POST_EVAR6                 	  VARCHAR(255) COMMENT '',
   POST_EVAR60	                  VARCHAR(255) COMMENT '',
   POST_EVAR61	                  VARCHAR(255) COMMENT '',
   POST_EVAR62	                  VARCHAR(255) COMMENT '',
   POST_EVAR63	                  VARCHAR(255) COMMENT '',
   POST_EVAR64	                  VARCHAR(255) COMMENT '',
   POST_EVAR65	                  VARCHAR(255) COMMENT '',
   POST_EVAR66	                  VARCHAR(255) COMMENT '',
   POST_EVAR67	                  VARCHAR(255) COMMENT '',
   POST_EVAR68	                  VARCHAR(255) COMMENT '',
   POST_EVAR69	                  VARCHAR(255) COMMENT '',
   POST_EVAR7                 	  VARCHAR(255) COMMENT '',
   POST_EVAR70	                  VARCHAR(255) COMMENT '',
   POST_EVAR71	                  VARCHAR(255) COMMENT '',
   POST_EVAR72	                  VARCHAR(255) COMMENT '',
   POST_EVAR73	                  VARCHAR(255) COMMENT '',
   POST_EVAR74	                  VARCHAR(255) COMMENT '',
   POST_EVAR75	                  VARCHAR(255) COMMENT '',
   POST_EVAR8                    VARCHAR(255) COMMENT '',
   POST_EVAR9                 	  VARCHAR(255) COMMENT '',
   POST_EVENT_LIST            	  VARCHAR(500) COMMENT '',
   POST_HIER1	                  VARCHAR(255) COMMENT '',
   POST_HIER2	                  VARCHAR(255) COMMENT '',
   POST_HIER3	                  VARCHAR(255) COMMENT '',
   POST_HIER4	                  VARCHAR(255) COMMENT '',
   POST_HIER5	                  VARCHAR(255) COMMENT '',
   POST_JAVA_ENABLED          	  CHAR(1) COMMENT '',
   POST_KEYWORDS              	  VARCHAR(255) COMMENT '',
   POST_MVVAR1	                  VARCHAR(255) COMMENT '',
   POST_MVVAR2	                  VARCHAR(255) COMMENT '',
   POST_MVVAR3	                  VARCHAR(255) COMMENT '',
   POST_PAGE_EVENT            	  DECIMAL(38,0) COMMENT '',
   POST_PAGE_EVENT_VAR1       	  VARCHAR(255) COMMENT '',
   POST_PAGE_EVENT_VAR2       	  VARCHAR(255) COMMENT '',
   POST_PAGE_EVENT_VAR3       	  VARCHAR(500) COMMENT '',
   POST_PAGE_TYPE             	  VARCHAR(255) COMMENT '',
   POST_PAGE_URL              	  VARCHAR(255) COMMENT '',
   POST_PAGENAME              	  VARCHAR(255) COMMENT '',
   POST_PAGENAME_NO_URL       	  VARCHAR(255) COMMENT '',
   POST_PARTNER_PLUGINS       	  VARCHAR(255) COMMENT '',
   POST_PERSISTENT_COOKIE     	  CHAR(1) COMMENT '',
   POST_PRODUCT_LIST          	  VARCHAR(255) COMMENT '',
   POST_PROP1	                  VARCHAR(255) COMMENT '',
   POST_PROP10	                  VARCHAR(255) COMMENT '',
   POST_PROP11	                  VARCHAR(255) COMMENT '',
   POST_PROP12	                  VARCHAR(255) COMMENT '',
   POST_PROP13	                  VARCHAR(255) COMMENT '',
   POST_PROP14	                  VARCHAR(255) COMMENT '',
   POST_PROP15	                  VARCHAR(255) COMMENT '',
   POST_PROP16	                  VARCHAR(255) COMMENT '',
   POST_PROP17	                  VARCHAR(255) COMMENT '',
   POST_PROP18	                  VARCHAR(255) COMMENT '',
   POST_PROP19	                  VARCHAR(255) COMMENT '',
   POST_PROP2	                  VARCHAR(255) COMMENT '',
   POST_PROP20	                  VARCHAR(255) COMMENT '',
   POST_PROP21	                  VARCHAR(255) COMMENT '',
   POST_PROP22	                  VARCHAR(255) COMMENT '',
   POST_PROP23	                  VARCHAR(255) COMMENT '',
   POST_PROP24	                  VARCHAR(255) COMMENT '',
   POST_PROP25	                  VARCHAR(255) COMMENT '',
   POST_PROP26	                  VARCHAR(255) COMMENT '',
   POST_PROP27	                  VARCHAR(255) COMMENT '',
   POST_PROP28	                  VARCHAR(255) COMMENT '',
   POST_PROP29	                  VARCHAR(255) COMMENT '',
   POST_PROP3	                  VARCHAR(255) COMMENT '',
   POST_PROP30	                  VARCHAR(255) COMMENT '',
   POST_PROP31	                  VARCHAR(255) COMMENT '',
   POST_PROP32	                  VARCHAR(255) COMMENT '',
   POST_PROP33	                  VARCHAR(255) COMMENT '',
   POST_PROP34	                  VARCHAR(255) COMMENT '',
   POST_PROP35	                  VARCHAR(255) COMMENT '',
   POST_PROP36	                  VARCHAR(255) COMMENT '',
   POST_PROP37	                  VARCHAR(255) COMMENT '',
   POST_PROP38	                  VARCHAR(255) COMMENT '',
   POST_PROP39	                  VARCHAR(255) COMMENT '',
   POST_PROP4	                  VARCHAR(255) COMMENT '',
   POST_PROP40	                  VARCHAR(255) COMMENT '',
   POST_PROP41	                  VARCHAR(255) COMMENT '',
   POST_PROP42	                  VARCHAR(255) COMMENT '',
   POST_PROP43	                  VARCHAR(255) COMMENT '',
   POST_PROP44	                  VARCHAR(255) COMMENT '',
   POST_PROP45	                  VARCHAR(255) COMMENT '',
   POST_PROP46	                  VARCHAR(255) COMMENT '',
   POST_PROP47	                  VARCHAR(255) COMMENT '',
   POST_PROP48	                  VARCHAR(255) COMMENT '',
   POST_PROP49	                  VARCHAR(255) COMMENT '',
   POST_PROP5	                  VARCHAR(255) COMMENT '',
   POST_PROP50	                  VARCHAR(255) COMMENT '',
   POST_PROP51	                  VARCHAR(255) COMMENT '',
   POST_PROP52	                  VARCHAR(255) COMMENT '',
   POST_PROP53	                  VARCHAR(255) COMMENT '',
   POST_PROP54	                  VARCHAR(255) COMMENT '',
   POST_PROP55	                  VARCHAR(255) COMMENT '',
   POST_PROP56	                  VARCHAR(255) COMMENT '',
   POST_PROP57	                  VARCHAR(255) COMMENT '',
   POST_PROP58	                  VARCHAR(255) COMMENT '',
   POST_PROP59	                  VARCHAR(255) COMMENT '',
   POST_PROP6	                  VARCHAR(255) COMMENT '',
   POST_PROP60	                  VARCHAR(255) COMMENT '',
   POST_PROP61	                  VARCHAR(255) COMMENT '',
   POST_PROP62	                  VARCHAR(255) COMMENT '',
   POST_PROP63	                  VARCHAR(255) COMMENT '',
   POST_PROP64	                  VARCHAR(255) COMMENT '',
   POST_PROP65	                  VARCHAR(255) COMMENT '',
   POST_PROP66	                  VARCHAR(255) COMMENT '',
   POST_PROP67	                  VARCHAR(255) COMMENT '',
   POST_PROP68	                  VARCHAR(255) COMMENT '',
   POST_PROP69	                  VARCHAR(255) COMMENT '',
   POST_PROP7	                  VARCHAR(255) COMMENT '',
   POST_PROP70	                  VARCHAR(255) COMMENT '',
   POST_PROP71	                  VARCHAR(255) COMMENT '',
   POST_PROP72	                  VARCHAR(255) COMMENT '',
   POST_PROP73	                  VARCHAR(255) COMMENT '',
   POST_PROP74	                  VARCHAR(255) COMMENT '',
   POST_PROP75	                  VARCHAR(255) COMMENT '',
   POST_PROP8	                  VARCHAR(255) COMMENT '',
   POST_PROP9	                  VARCHAR(255) COMMENT '',
   POST_PURCHASEID            	  VARCHAR(255) COMMENT '',
   POST_REFERRER              	  VARCHAR(255) COMMENT '',
   POST_SEARCH_ENGINE         	  DECIMAL(38,0) COMMENT '',
   POST_STATE                 	  VARCHAR(255) COMMENT '',
   POST_SURVEY                	  VARCHAR(500) COMMENT '',
   POST_T_TIME_INFO           	  VARCHAR(255) COMMENT '',
   POST_TNT                   	  VARCHAR(500) COMMENT '',
   POST_TRANSACTIONID         	  VARCHAR(255) COMMENT '',
   POST_VISID_HIGH            	  VARCHAR(20) COMMENT '',
   POST_VISID_LOW             	  VARCHAR(20) COMMENT '',
   POST_VISID_TYPE            	  DECIMAL(38,0) COMMENT '',
   POST_ZIP                   	  VARCHAR(255) COMMENT '',
   PREV_PAGE                  	  DECIMAL(38,0) COMMENT '',
   PRODUCT_LIST               	  VARCHAR(500) COMMENT '',
   PRODUCT_MERCHANDISING      	  VARCHAR(500) COMMENT '',
   PROP1	                      VARCHAR(255) COMMENT '',
   PROP11_OPTIMUM_ID           	  VARCHAR(255) COMMENT '',
   PROP12	                      VARCHAR(255) COMMENT '',
   PROP13	                      VARCHAR(255) COMMENT '',
   PROP14	                      VARCHAR(255) COMMENT '',
   PROP15	                      VARCHAR(255) COMMENT '',
   PROP16	                      VARCHAR(255) COMMENT '',
   PROP17	                      VARCHAR(255) COMMENT '',
   PROP18	                      VARCHAR(255) COMMENT '',
   PROP19	                      VARCHAR(255) COMMENT '',
   PROP2	                      VARCHAR(255) COMMENT '',
   PROP20	                      VARCHAR(255) COMMENT '',
   PROP21	                      VARCHAR(255) COMMENT '',
   PROP22	                      VARCHAR(255) COMMENT '',
   PROP23	                      VARCHAR(255) COMMENT '',
   PROP24	                      VARCHAR(255) COMMENT '',
   PROP25_HULU_ID              	  VARCHAR(255) COMMENT '',
   PROP26	                      VARCHAR(255) COMMENT '',
   PROP27	                      VARCHAR(255) COMMENT '',
   PROP28	                      VARCHAR(255) COMMENT '',
   PROP29	                      VARCHAR(255) COMMENT '',
   PROP3	                      VARCHAR(255) COMMENT '',
   PROP30	                      VARCHAR(255) COMMENT '',
   PROP31	                      VARCHAR(255) COMMENT '',
   PROP32	                      VARCHAR(255) COMMENT '',
   PROP33	                      VARCHAR(255) COMMENT '',
   PROP34	                      VARCHAR(255) COMMENT '',
   PROP35_OPT_ERR_MSG_DESC        VARCHAR(255) COMMENT '',
   PROP36	                      VARCHAR(255) COMMENT '',
   PROP37	                      VARCHAR(255) COMMENT '',
   PROP38	                      VARCHAR(255) COMMENT '',
   PROP39	                      VARCHAR(255) COMMENT '',
   PROP4	                      VARCHAR(255) COMMENT '',
   PROP40	                      VARCHAR(255) COMMENT '',
   PROP41	                      VARCHAR(255) COMMENT '',
   PROP42	                      VARCHAR(255) COMMENT '',
   PROP43	                      VARCHAR(255) COMMENT '',
   PROP44	                      VARCHAR(255) COMMENT '',
   PROP45	                      VARCHAR(255) COMMENT '',
   PROP46	                      VARCHAR(255) COMMENT '',
   PROP47	                      VARCHAR(255) COMMENT '',
   PROP48	                      VARCHAR(255) COMMENT '',
   PROP49	                      VARCHAR(255) COMMENT '',
   PROP5	                      VARCHAR(255) COMMENT '',
   PROP50	                      VARCHAR(255) COMMENT '',
   PROP51	                      VARCHAR(255) COMMENT '',
   PROP52	                      VARCHAR(255) COMMENT '',
   PROP53	                      VARCHAR(255) COMMENT '',
   PROP54	                      VARCHAR(255) COMMENT '',
   PROP55	                      VARCHAR(255) COMMENT '',
   PROP56	                      VARCHAR(255) COMMENT '',
   PROP57	                      VARCHAR(255) COMMENT '',
   PROP58	                      VARCHAR(255) COMMENT '',
   PROP59	                      VARCHAR(255) COMMENT '',
   PROP6	                      VARCHAR(255) COMMENT '',
   PROP60	                      VARCHAR(255) COMMENT '',
   PROP61	                      VARCHAR(255) COMMENT '',
   PROP62	                      VARCHAR(255) COMMENT '',
   PROP63	                      VARCHAR(255) COMMENT '',
   PROP64	                      VARCHAR(255) COMMENT '',
   PROP65	                      VARCHAR(255) COMMENT '',
   PROP66	                      VARCHAR(255) COMMENT '',
   PROP67	                      VARCHAR(255) COMMENT '',
   PROP68	                      VARCHAR(255) COMMENT '',
   PROP69	                      VARCHAR(255) COMMENT '',
   PROP7	                      VARCHAR(255) COMMENT '',
   PROP70	                      VARCHAR(255) COMMENT '',
   PROP71	                      VARCHAR(255) COMMENT '',
   PROP72	                      VARCHAR(255) COMMENT '',
   PROP73	                      VARCHAR(255) COMMENT '',
   PROP74	                      VARCHAR(255) COMMENT '',
   PROP75	                      VARCHAR(255) COMMENT '',
   PROP8	                      VARCHAR(255) COMMENT '',
   PROP9	                      VARCHAR(255) COMMENT '',
   PURCHASEID                 	  VARCHAR(255) COMMENT '',
   QUARTERLY_VISITOR          	  DECIMAL(38,0) COMMENT '',
   REF_DOMAIN                 	  VARCHAR(255) COMMENT '',
   REFERRER                   	  VARCHAR(255) COMMENT '',
   REFERRER_TYPE_FULLNAME     	  VARCHAR(100) COMMENT 'FULL NAME OF REFERRER TYPE',
   REFERRER_TYPE_ID           	  DECIMAL(38,0) COMMENT '',  
   REFERRER_TYPE_SHORTNAME    	  VARCHAR(100) COMMENT 'SHORT  NAME OF REFERRER TYPE',
   RESOLUTION                 	  DECIMAL(38,0) COMMENT '',
   S_RESOLUTION               	  VARCHAR(255) COMMENT '',
   SAMPLED_HIT                	  CHAR(1) COMMENT '',
   SEARCH_ENGINE_ID           	  DECIMAL(38,0) COMMENT '',  
   SEARCH_ENGINE_NAME         	  VARCHAR(100) COMMENT 'NAME OF SEARCH ENGINE ',
   SEARCH_PAGE_NUM            	  VARCHAR(255) COMMENT '',
   SECONDARY_HIT              	  VARCHAR(255) COMMENT '',
   SERVICE                    	  CHAR(2) COMMENT '',
   SITE_SECTION_ID            	  DECIMAL(38,0) COMMENT '',
   SITE_SECTION_NAME          	  VARCHAR(255) COMMENT '',
   SOURCED_FROM_SYSTEM        	  STRING COMMENT '',
   SOURCEID                   	  DECIMAL(38,0) COMMENT '',
   STATE                      	  VARCHAR(255) COMMENT '',
   STATS_SERVER               	  VARCHAR(255) COMMENT '',
   SUITE_ID                   	  INT,
   T_TIME_INFO                	  VARCHAR(255) COMMENT '',
   TNT                        	  VARCHAR(500) COMMENT '',
   TNT_POST_VISTA             	  VARCHAR(500) COMMENT '',
   TRANSACTIONID              	  VARCHAR(255) COMMENT '',
   TRUNCATED_HIT              	  CHAR(1) COMMENT '',
   UA_COLOR                   	  VARCHAR(255) COMMENT '',
   UA_OS                      	  VARCHAR(255) COMMENT '',
   UA_PIXELS                  	  VARCHAR(255) COMMENT '',
   USER_AGENT                 	  VARCHAR(500) COMMENT '',
   USER_HASH                  	  DECIMAL(38,0) COMMENT '',
   USER_SERVER                	  VARCHAR(255) COMMENT '',
   USERID                     	  DECIMAL(38,0) COMMENT '',
   USERNAME                   	  VARCHAR(40) COMMENT '',
   VA_CLOSER_DETAIL           	  VARCHAR(255) COMMENT '',
   VA_CLOSER_ID               	  DECIMAL(38,0) COMMENT '',
   VA_FINDER_DETAIL           	  VARCHAR(255) COMMENT '',
   VA_FINDER_ID               	  DECIMAL(38,0) COMMENT '',
   VA_INSTANCE_EVENT          	  DECIMAL(38,0) COMMENT '',
   VA_NEW_ENGAGEMENT          	  DECIMAL(38,0) COMMENT '',
   VISID_HIGH                 	  VARCHAR(20) COMMENT '',
   VISID_LOW                  	  VARCHAR(20) COMMENT '',
   VISID_NEW                  	  CHAR(1) COMMENT '',
   VISID_TIMESTAMP            	  DECIMAL(38,0) COMMENT '',
   VISID_TYPE                 	  DECIMAL(38,0) COMMENT '',
   VISIT_DATE                 	  TIMESTAMP COMMENT '',
   VISIT_KEYWORDS             	  VARCHAR(255) COMMENT '',
   VISIT_NUM                  	  DECIMAL(38,0) COMMENT '',
   VISIT_PAGE_NUM             	  DECIMAL(38,0) COMMENT '',
   VISIT_REFERRER             	  VARCHAR(255) COMMENT '',
   VISIT_SEARCH_ENGINE        	  DECIMAL(38,0) COMMENT '',
   VISIT_START_PAGE_URL       	  VARCHAR(255) COMMENT '',
   VISIT_START_PAGENAME       	  VARCHAR(255) COMMENT '',
   VISIT_START_TIME_GMT       	  DECIMAL(38,0) COMMENT '',
   WEEKLY_VISITOR             	  DECIMAL(38,0) COMMENT '',
   YEARLY_VISITOR             	  DECIMAL(38,0) COMMENT '',
   ZIP                        	  VARCHAR(255) COMMENT ''
)
PARTITIONED BY (SUITE_NAME STRING,
                SOURCE_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################