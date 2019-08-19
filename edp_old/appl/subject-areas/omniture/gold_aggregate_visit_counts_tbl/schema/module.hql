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

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
  COUNT_TYPE 			VARCHAR(30)  	COMMENT '',
  COUNT_TYPE_ID 		BIGINT		 	COMMENT '',
  COUNT_TYPE_VALUE 		VARCHAR(500)    COMMENT '',
  VISIT_COUNT 			BIGINT  		COMMENT '',
  DTM_CREATED 			TIMESTAMP 	 	COMMENT '',
  SOURCED_FROM_SYSTEM 	VARCHAR(255)  	COMMENT ''
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