--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Smith derived lookup table 
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



CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}smith_optimum_isp
(    
   ISP_ID                    DECIMAL(38,0) 	COMMENT 'The ISP ID (Has lookup table) actually used for the hit.',
   ISP_NAME                  VARCHAR(255) 	COMMENT 'The ISP NAME (Has lookup table) actually used for the hit.',
   DTM_START                 STRING 		COMMENT 'date_time field from hit_data',      
   DTM_END                   STRING 		COMMENT 'default value 31-Dec-99', 
   SOURCED_FROM_SYSTEM       VARCHAR(60) 	COMMENT 'Capture source name.',
   DTM_CREATED               TIMESTAMP 		COMMENT 'The DATETIME when record created.'
 )
PARTITIONED BY (SUITE_ID STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}/smith_optimum_isp'
;

--##############################################################################
--## Creating smith_pagename table
--##############################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}smith_optimum_pagenames
(    
   PAGE_NAME_ID             DECIMAL(38,0) 	COMMENT 'The PAGE NAME ID (Has lookup table) actually used for the hit.',
   PAGENAME                 VARCHAR(255) 	COMMENT 'The PAGE NAME (Has lookup table) actually used for the hit.',
   DTM_START                STRING 			COMMENT 'date field from hit_data',  
   DTM_END                  STRING 			COMMENT 'default value 31-Dec-99',    
   SOURCED_FROM_SYSTEM      VARCHAR(60) 	COMMENT 'Capture source name.',
   DTM_CREATED              TIMESTAMP 		COMMENT 'The DATETIME when record created.'
 )
PARTITIONED BY (SUITE_ID STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}/smith_optimum_pagenames'
;

--##############################################################################
--## Creating smith_site_section table
--##############################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}smith_optimum_site_sections
(    
   SITE_SECTION_ID           DECIMAL(38,0) 	COMMENT 'The ISP ID (Has lookup table) actually used for the hit.',
   SITE_SECTION_NAME         VARCHAR(255) 	COMMENT 'The ISP NAME (Has lookup table) actually used for the hit.',
   DTM_START                 STRING 		COMMENT 'date_time field from hit_data',
   DTM_END                   STRING 		COMMENT 'default value 31-Dec-99',    
   SOURCED_FROM_SYSTEM       VARCHAR(60) 	COMMENT 'Capture source name.',
   DTM_CREATED               TIMESTAMP 		COMMENT 'The DATETIME when record created.'
 )
PARTITIONED BY (SUITE_ID STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}/smith_optimum_site_sections'
;

--##############################################################################
--## Creating Smith_optimum_error_message table
--##############################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}smith_optimum_err_message
(    
   OPT_ERR_MSG_ID            DECIMAL(38,0) 		COMMENT 'The error message id used for the hit',
   OPT_ERR_MSG_DESC          VARCHAR(100) 		COMMENT 'The error message description used for the hit',
   DTM_START                 STRING 		COMMENT 'date_time field from hit_data',
   DTM_END                   STRING 		COMMENT 'default value 31-Dec-99',            
   SOURCED_FROM_SYSTEM       VARCHAR(60) 		COMMENT 'Capture source name',
   DTM_CREATED               TIMESTAMP 			COMMENT 'The DATETIME when record created'
 )
PARTITIONED BY (SUITE_ID STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}/smith_optimum_err_message'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################	