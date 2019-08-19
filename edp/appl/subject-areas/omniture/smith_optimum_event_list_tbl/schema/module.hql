--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Smith Optimum event list table 
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

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
    SUITE_ID 	     			INT 			COMMENT '',
    EVENT_LIST_ID      			INT             COMMENT '',
    EVENT_LIST_DESCRIPTION      VARCHAR(100) 	COMMENT '',
    DTM_EFFTV        			DATE 			COMMENT '',
    DTM_EXPIRED       			DATE 			COMMENT '',
    DTM_CREATED      			TIMESTAMP 		COMMENT '',
    SOURCED_FROM_SYSTEM 		VARCHAR(60) 	COMMENT '',
    DTM_LAST_UPDATED    		TIMESTAMP 		COMMENT '',
    LAST_UPDATED_BY_SYSTEM 		VARCHAR(60) 	COMMENT '' 
)

ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
TBLPROPERTIES ("skip.header.line.count"="1")
;

--##############################################################################
--#                                    End                                     #
--##############################################################################