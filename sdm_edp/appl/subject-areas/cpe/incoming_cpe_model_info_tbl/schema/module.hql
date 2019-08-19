--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create cpe_model_info table at Incoming layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/24/2017
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
--#    1.0     DataMetica Team          01/24/2017       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
 MODEL                  STRING        COMMENT '',
 MODEL_DESC             STRING        COMMENT '',
 LOB                    STRING        COMMENT '',
 CATEGORY               STRING        COMMENT '',
 MAKE                   STRING        COMMENT '',
 ACTIVE                 STRING        COMMENT '',
 RYG_STATUS             STRING        COMMENT '',
 CPE_FORECAST_CATEGORY  STRING        COMMENT '',
 M2_OR_M4               STRING        COMMENT '',
 MODEL_TRIM             STRING        COMMENT '',
 MODEL_CATEGORY         STRING        COMMENT '',
 DOCSIS                 STRING        COMMENT ''
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################