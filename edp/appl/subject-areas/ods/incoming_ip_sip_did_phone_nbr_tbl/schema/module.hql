--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create table incoming_ip_sip_did_phone_nbr
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2016
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
--#    1.0     DataMetica Team          12/28/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
  SIP_DID_PHONE_NBR_ID       BIGINT    COMMENT '',
  SIP_PILOT_PHONE_NBR_ID     BIGINT    COMMENT '',
  ID_SIP_PILOT               BIGINT    COMMENT '',
  ID_IPTN                    BIGINT    COMMENT '',
  PHONE_NBR_ID               BIGINT    COMMENT '',
  ID_BLOCK                   BIGINT    COMMENT '',
  ID_ORDER_STATUS            BIGINT    COMMENT '',
  PILOT_PHONE_NBR_IND        CHAR(1)   COMMENT '',
  LEAD_PHONE_NBR_IND         CHAR(1)   COMMENT '',
  AREA_CODE                  CHAR(3)   COMMENT '',
  PHONE_EXCHANGE             CHAR(3)   COMMENT '',
  EXCHANGE_EXTENTION         CHAR(4)   COMMENT '',
  CUSTOMER_ACCOUNT_ID        BIGINT    COMMENT '',
  CORP                       BIGINT    COMMENT '',
  DWELLING_NBR               VARCHAR(6)  COMMENT '',
  CUST                       VARCHAR(2)  COMMENT '',
  CREATED_BY                 VARCHAR(20)  COMMENT '',
  CREATE_DATE                TIMESTAMP   COMMENT '',
  LAST_UPDATED_BY            VARCHAR(20)  COMMENT '',
  LAST_UPDATE_DATE           TIMESTAMP   COMMENT '',
  ARCHIVED_BY                VARCHAR(20)  COMMENT '',
  ARCHIVE_DATE               TIMESTAMP   COMMENT '',
  DTM_OF_ACTIVATION          TIMESTAMP   COMMENT '',
  DTM_OF_INIT_CREATION       TIMESTAMP   COMMENT '',
  DTM_EFFTV                  TIMESTAMP   COMMENT '',
  DTM_EXPIRED                TIMESTAMP   COMMENT '',
  DTM_CREATED              TIMESTAMP      COMMENT '',
  DTM_LAST_UPDATED           TIMESTAMP   COMMENT '',
  SOURCED_FROM_SYSTEM        VARCHAR(50)  COMMENT '',
  LAST_UPDATED_BY_SYSTEM     VARCHAR(50)  COMMENT ''
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################