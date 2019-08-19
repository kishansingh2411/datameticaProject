--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Smith KOM stock36 optimum usage table 
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
	ACCOUNT_NUMBER              VARCHAR(255)     COMMENT '',
	BROWSER_ID                  DECIMAL(38,0)    COMMENT '',
	CAMPAIGN                    VARCHAR(255)     COMMENT '',
	CONNECTION_TYPE_ID          DECIMAL(38,0)    COMMENT '',
	CORP                        VARCHAR(5)       COMMENT '',
	COUNTRY_ID                  DECIMAL(38,0)    COMMENT '',
	CUST                        VARCHAR(2)       COMMENT '',
	DTM_CREATED                 TIMESTAMP        COMMENT '',
	DTM_LAST_UPDATED            TIMESTAMP        COMMENT '',
	DWELLING_NBR                VARCHAR(6)       COMMENT '',
	ISP_ID                      DECIMAL(38,0)    COMMENT '',
	JAVASCRIPT_VERSION_ID       DECIMAL(38,0)    COMMENT '',
	LANGUAGE_ID                 DECIMAL(38,0)    COMMENT '',
	LAST_UPDATED_BY_SYSTEM      TIMESTAMP        COMMENT '',
	MAC_ADDRESS                 VARCHAR(255)     COMMENT '',
	MONTHLY_VISITOR             DECIMAL(38,0)    COMMENT '',
	OPERATING_SYSTEM_ID         DECIMAL(38,0)    COMMENT '',
	PAGE_NAME_ID                DECIMAL(38,0)    COMMENT '',
	PAGE_TYPE                   VARCHAR(255)     COMMENT '',
	PAGE_URL                    VARCHAR(500)     COMMENT '',
	PLUGINS                     VARCHAR(255)     COMMENT '',
	SEARCH_ENGINE_ID            DECIMAL(38,0)    COMMENT '',
	SITE_SECTION_ID             DECIMAL(38,0)    COMMENT '',
	SOURCED_FROM_SYSTEM         VARCHAR(60)      COMMENT '',
	SUITE_ID                    VARCHAR(4)       COMMENT '',
	VISID_HIGH                  VARCHAR(20)      COMMENT '',
	VISID_LOW                   VARCHAR(20)      COMMENT '',
	VISIT_DATE                  VARCHAR(20)      COMMENT '',
	VISIT_NUM                   DECIMAL(38,0)    COMMENT '',
	VISIT_START_TIME_GMT        DECIMAL(38,0)    COMMENT '',
	VISIT_PAGE_NUM              DECIMAL(38,0)    COMMENT '',
	VOD_CATEGORY                VARCHAR(255)     COMMENT '',
	VOD_TITLE                   VARCHAR(255)     COMMENT '',
	VOD_PLAY_TIME               VARCHAR(256)     COMMENT '',
	VOID_ID                     VARCHAR(255)     COMMENT ''
)

PARTITIONED BY (SOURCE_DATE STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
LOCATION '${hivevar:location}'
;
--##############################################################################
--#                                    End                                     #
--##############################################################################