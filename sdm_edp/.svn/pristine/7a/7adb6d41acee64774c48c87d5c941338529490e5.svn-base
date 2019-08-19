--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_fwm_house_eqmt_data table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/30/2017
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
--#    1.0     DataMetica Team          01/30/2017       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
SITE_ID                 INT                            COMMENT '',
ACCT_NBR                BIGINT                         COMMENT '',
HOUSEHOLD_ID            STRING                         COMMENT '',
LAST_NAME               STRING                         COMMENT '',
FIRST_NAME              STRING                         COMMENT '',
STREET_ADDRESS          STRING                         COMMENT '',
CITY                    STRING                         COMMENT '',
STATE                   STRING                         COMMENT '',
ZIP_CODE                STRING                         COMMENT '',
SET_TOP_BOX_ID          STRING                         COMMENT '',
ZIP                     STRING                         COMMENT '',
EMPLOYEE_STATUS         INT                            COMMENT '',
ACCOUNT_STATUS          INT                            COMMENT '',
SET_TOP_BOX_MODEL       STRING                         COMMENT '',
ZONE_NAME               STRING                         COMMENT '',
NODE_ID                 STRING                         COMMENT '',
PREMIUM_CHANNELS        INT                            COMMENT '',
SPORTS_PACKAGES         STRING                         COMMENT '',
LANGUAGE_CHANNELS       STRING                         COMMENT '',
DVR                     INT                            COMMENT '',
HDTV                    INT                            COMMENT '',
CABLE_MODEM_IP_ADDRESS  STRING                         COMMENT '',
ZIP_PLUS_4              STRING                         COMMENT '',
FRANCHISE_NAME          INT                            COMMENT '',
HEADEND_ID              STRING                         COMMENT '',
HUB_ID                  INT                            COMMENT '',
TIER                    STRING                         COMMENT '',
SEGMENTATION_CODE       STRING                         COMMENT '',
REGION                  STRING                         COMMENT '',
PRISM_CODE              STRING                         COMMENT '',
SYSTEM_NAME             STRING                         COMMENT '',
TIVO                    INT                            COMMENT '',
TIVO_TSN                STRING                         COMMENT '',
HSD                     INT                            COMMENT '',
TELEPHONE               INT                            COMMENT '',
SECURITY                INT                            COMMENT '',
MAC_ADDRESS             STRING                         COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################