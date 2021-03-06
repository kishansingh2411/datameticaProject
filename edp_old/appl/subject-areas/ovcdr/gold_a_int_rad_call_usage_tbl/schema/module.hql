--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create aggregate table(a_vma_nsn_call_usage)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/bering_media/BERING_MEDIA_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/bering_media/BERING_MEDIA_DEPLOYMENT.log
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
  TELEPHONE_NUMBER   VARCHAR(10) COMMENT '',
  PRODUCT_NAME       VARCHAR(10) COMMENT '',
  PLAN_NUMBER        VARCHAR(10) COMMENT '',
  SECONDS_OF_USE     INT COMMENT '',
  NUMBER_OF_CALLS    INT COMMENT '',
  BATCH_NBR          INT COMMENT '',
  ATTEMPT_COUNT      INT COMMENT '',
  CORP               INT COMMENT '',
  HOUSE              VARCHAR(6) COMMENT '',
  CUST               VARCHAR(2) COMMENT '',
  DTM_CREATED        TIMESTAMP COMMENT '',
  DTM_LAST_MODIFIED  TIMESTAMP COMMENT '',
  FILE_NAME          VARCHAR(60) COMMENT '',
  TERM_COUNTRY       VARCHAR(10) COMMENT ''
)
PARTITIONED BY (USAGE_DATE STRING)
CLUSTERED BY (TELEPHONE_NUMBER) INTO 1 BUCKETS
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################