--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create work_acct_srv_occur_lob_tbl at Work layer
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
  SITE_ID                 INT               COMMENT '',
  ACCT_NBR                BIGINT            COMMENT '',
  CUST_GROUP_CD           STRING            COMMENT '',
  SERVICE_OCCURRENCE      INT               COMMENT '',
  SVC_CTGY_CD             STRING            COMMENT '',
  ACTIVE_SRV_FLAG         INT               COMMENT '',
  ACTIVE_SVC_CTGY_C       INT               COMMENT '',
  ACTIVE_SVC_CTGY_D       INT               COMMENT '',
  ACTIVE_SVC_CTGY_H       INT               COMMENT '',
  ACTIVE_SVC_CTGY_S       INT               COMMENT '',
  ACTIVE_SVC_CTGY_T       INT               COMMENT '',
  LOB_DATA                INT               COMMENT '',
  LOB_HOME                INT               COMMENT '',
  LOB_CABLE               INT               COMMENT '',
  LOB_PHONE               INT               COMMENT '',
  P_YYYYMMDD              STRING            COMMENT ''
)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################