--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_cpe_ivr table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/20/2017
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
--#    1.0     DataMetica Team          01/20/2017       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
 SITE_ID                   INT        COMMENT '',
 ACCT_NBR                  BIGINT     COMMENT '',
 IVR_TECH_TOTAL            INT        COMMENT '',
 IVR_TECH_CONTAIN          INT        COMMENT '',
 IVR_TECH_OFFER            INT        COMMENT '',
 IVR_TECH_VIDEO_TOTAL      INT        COMMENT '',
 IVR_TECH_VIDEO_CONTAIN    INT        COMMENT '',
 IVR_TECH_VIDEO_OFFER      INT        COMMENT '',
 IVR_TECH_DATA_TOTAL       INT        COMMENT '',
 IVR_TECH_DATA_CONTAIN     INT        COMMENT '',
 IVR_TECH_DATA_OFFER       INT        COMMENT '',
 IVR_TECH_PHONE_TOTAL      INT        COMMENT '',
 IVR_TECH_PHONE_CONTAIN    INT        COMMENT '',
 IVR_TECH_PHONE_OFFER      INT        COMMENT ''
) 
PARTITIONED BY (P_YYYYMMDD    STRING) 
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################