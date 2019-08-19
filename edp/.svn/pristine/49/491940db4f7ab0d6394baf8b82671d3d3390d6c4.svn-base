--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create work_bhv_acct_phone_nbr_opt_sdl table at work layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/02/2017
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
--#    1.0     DataMetica Team          03/02/2017       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE TABLE 
   ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   PHONE_NBR           VARCHAR(10)
   ,SOURCE_SYSTEM_ID   TINYINT
   ,CUST_ACCT_NUMBER   VARCHAR(15)
   ,CORP               INT
   ,HOUSE              VARCHAR(6)
   ,CUST               VARCHAR(2)
   ,DTM_EFFTV          DATE
   ,DTM_EXPIRED        DATE
   ,DTM_CREATED        TIMESTAMP
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################