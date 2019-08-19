--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_bhv_acct_phone_nbr_sdl_tmp)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/01/2017
--#   Log File    : .../log/customer/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/customer/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/01/2017       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE TABLE 
   ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   GOLD_BHV_ACCT_PHONE_NBR_SDL_ID   BIGINT
   ,WORK_BHV_ACCT_PHONE_NBR_SDL_ID   BIGINT
   ,GOLD_ACCT_NBR                    INT
   ,WORK_ACCT_NBR                    INT
   ,GOLD_PHONE_NBR                   VARCHAR(10)
   ,WORK_PHONE_NBR                   VARCHAR(10)
   ,GOLD_DTM_EFFTV                   TIMESTAMP
   ,WORK_DTM_EFFTV                   TIMESTAMP
   ,GOLD_SITE_ID                     INT
   ,WORK_SITE_ID                     INT
   ,SITE_ID_FLAG                     VARCHAR(1)
   ,GOLD_AREA_CODE                   VARCHAR(3)
   ,WORK_AREA_CODE                   VARCHAR(3)
   ,AREA_CODE_FLAG                   VARCHAR(1)
   ,GOLD_PHONE_EXCHANGE              VARCHAR(3)
   ,WORK_PHONE_EXCHANGE              VARCHAR(3)
   ,PHONE_EXCHANGE_FLAG              VARCHAR(1)
   ,GOLD_EXCHANGE_EXTENTION          VARCHAR(4)
   ,WORK_EXCHANGE_EXTENTION          VARCHAR(4)
   ,EXCHANGE_EXTENTION_FLAG          VARCHAR(1)
   ,GOLD_CUST_PHONE_STS              VARCHAR(2)
   ,WORK_CUST_PHONE_STS              VARCHAR(2)
   ,CUST_PHONE_STS_FLAG              VARCHAR(1)
   ,GOLD_CUSTOMER_TN_TYPE            VARCHAR(6)
   ,WORK_CUSTOMER_TN_TYPE            VARCHAR(6)
   ,CUSTOMER_TN_TYPE_FLAG            VARCHAR(1)
   ,GOLD_SRVC_CLASS                  VARCHAR(2)
   ,WORK_SRVC_CLASS                  VARCHAR(2)
   ,SRVC_CLASS_FLAG                  VARCHAR(1)
   ,GOLD_DTM_EXPIRED                 TIMESTAMP
   ,WORK_DTM_EXPIRED                 TIMESTAMP
   ,DTM_EXPIRED_FLAG                 VARCHAR(1)
   ,GOLD_CUST_TEL_INSTALL_DT         INT
   ,WORK_CUST_TEL_INSTALL_DT         INT
   ,CUST_TEL_INSTALL_DT_FLAG         VARCHAR(1)
   ,GOLD_CUST_TEL_DISC_DT            INT
   ,WORK_CUST_TEL_DISC_DT            INT
   ,CUST_TEL_DISC_DT_FLAG            VARCHAR(1)
   ,GOLD_CUST_TEL_INSTALL_TM         INT
   ,WORK_CUST_TEL_INSTALL_TM         INT
   ,CUST_TEL_INSTALL_TM_FLAG         VARCHAR(1)
   ,GOLD_CUST_TEL_DISC_TM            INT
   ,WORK_CUST_TEL_DISC_TM            INT
   ,CUST_TEL_DISC_TM_FLAG            VARCHAR(1)
   ,GOLD_DTM_TEL_INSTALL             TIMESTAMP
   ,WORK_DTM_TEL_INSTALL             TIMESTAMP
   ,DTM_TEL_INSTALL_FLAG             VARCHAR(1)
   ,GOLD_DTM_TEL_DISCONNECT          TIMESTAMP
   ,WORK_DTM_TEL_DISCONNECT          TIMESTAMP
   ,DTM_TEL_DISCONNECT_FLAG          VARCHAR(1)
   ,DTM_CREATED                      TIMESTAMP
   ,DTM_LAST_MODIFIED                TIMESTAMP
   ,NEW_FLAG                         VARCHAR(1)
   ,CHANGED_FLAG                     VARCHAR(1)
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################