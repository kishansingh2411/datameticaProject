--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_ov_telephone_number_tbl)
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
  OV_TELEPHONE_NUMBER_ID    DECIMAL(38,0)   COMMENT '',
  CORP                      INT              COMMENT '',
  TELEPHONE_NUMBER          VARCHAR(10)      COMMENT '',
  AREA_CODE                 VARCHAR(3)       COMMENT '',
  PHONE_EXCHANGE            VARCHAR(3)       COMMENT '',
  EXCHANGE_EXTENTION        VARCHAR(4)       COMMENT '',
  CUSTOMER_ACCOUNT_ID       INT              COMMENT '',
  ID_IPTEL_ORDER            INT              COMMENT '',
  SOURCED_FROM_SYSTEM       VARCHAR(63)      COMMENT '',
  LAST_UPDATED_FROM_SYSTEM  VARCHAR(63)      COMMENT '',
  DTM_CREATED               TIMESTAMP        COMMENT '',
  DTM_EFFTV                 TIMESTAMP        COMMENT '',
  DTM_EXPIRED               TIMESTAMP        COMMENT '',
  DTM_LAST_MODIFIED         TIMESTAMP        COMMENT '',
  DTM_LAST_UPDATED          TIMESTAMP        COMMENT ''
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################