--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create metadata table(gold_metadata_vma_nsn_call_usage) at Gold layer.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/08/2016
--#   Log File    : .../log/ovcdr/OVCDR_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/ovcdr/OVCDR_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/08/2016       Initial version
--#
--#
--#####################################################################################################################

CREATE TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   BATCH_NBR          VARCHAR(10)               COMMENT '',
   USAGE_DATE         VARCHAR(10)               COMMENT ''
)
CLUSTERED BY (BATCH_NBR) INTO 1 BUCKETS
STORED AS ORC 
LOCATION '${hivevar:location}'
TBLPROPERTIES('transactional'='true')
;

--##############################################################################
--#                                    End                                     #
--##############################################################################