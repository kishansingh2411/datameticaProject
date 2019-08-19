--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_export_cmts_node_tbl table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 02/05/2017
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
--#    1.0     DataMetica Team          02/05/2017       Initial version
--#
--#
--#####################################################################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
  CMTS_HOSTNAME                   STRING        COMMENT '',
  MACADDR                         STRING        COMMENT '',
  IFDESCR                         STRING        COMMENT '',
  IFNAME                          STRING        COMMENT '',
  IFALIAS                         STRING        COMMENT '',
  SITE_ID                         STRING        COMMENT '',
  HOUSE_NBR                       STRING        COMMENT '',
  ACCT_NBR                        STRING        COMMENT '',
  ICOMS_NODE                      STRING        COMMENT '',
  ICOMS_HEADEND                   STRING        COMMENT '',
  TIME_OF_UPDATE                  STRING        COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD string)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################