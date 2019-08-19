--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Languages table in Gold layer
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

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   VISIT_DATE   			DATE 		 	COMMENT 'DATE_TIME from HIT_DELTA',
   COUNT_TYPE  		        VARCHAR(50)  	COMMENT 'PAGENAME/SITESECTION',
   COUNT_TYPE_ID            INT 		 	COMMENT 'key from dimension table',
   COUNT_TYPE_VALUE 		VARCHAR(750) 	COMMENT 'actual pagename/sitesection name',
   VISID_COUNT              INT 		 	COMMENT 'DISTINCT of POS_VISID_HIGH||*||POS_VISID_LOW',
   OPTIMUM_ID_COUNT         INT          	COMMENT 'Same as above but where OPTIMUM_ID is NOT NULL',
   CORP_HOUSE_CUST_COUNT    INT 		 	COMMENT 'Same as above but where CORP_HOUSE_CUST is NOT NULL',
   PERIOD_START_DATE        DATE 		 	COMMENT 'LOOKUP(D_PERIOD,VISIT_DATE) GET  week_start_date (Ref D_PERIOD_LOOKUP tab)',
   PERIOD_END_DATE          DATE 		 	COMMENT 'LOOKUP(D_PERIOD,VISIT_DATE) GET  week_end_date (Ref D_PERIOD_LOOKUP tab)',
   DTM_CREATED              TIMESTAMP    	COMMENT 'sysdate',
   SOURCED_FROM_SYSTEM      VARCHAR(255) 	COMMENT 'omniture',
   SITE_TYPE                VARCHAR(255)     COMMENT 'to determine business or residential'
)
PARTITIONED BY (SUITE_NAME STRING, 
                FREQUENCY STRING, 
                PERIOD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################