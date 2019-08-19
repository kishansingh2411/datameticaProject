--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_bhv_acct_phone_nbr_sdl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 02/01/2017
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
--#    1.0     DataMetica Team          02/01/2017       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE TABLE 
   ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   BHV_ACCT_PHONE_NBR_SDL_ID     BIGINT        COMMENT 'PK of this table'
   ,SITE_ID                      INT           COMMENT 'Site id - Sourced from CUSTOMER_TELEPHONE.SITE_ID IS GTNROV'
   ,ACCT_NBR                     INT           COMMENT 'Account Number - Sourced from CUSTOMER_TELEPHONE.ACCT_NBR IS GTCNBR'
   ,PHONE_NBR                    VARCHAR(10)   COMMENT 'lpad(CUST_TELEPHONE_NBR.NPA_STD,3,0)||lpad(CUST_TELEPHONE_NBR.EXCHANGE,3,0)||lpad(CUST_TELEPHONE_NBR.TN_LINE_NBR,4,0)'
   ,AREA_CODE                    VARCHAR(3)    COMMENT 'lpad(CUST_TELEPHONE_NBR.NPA_STD,3,0)'
   ,PHONE_EXCHANGE               VARCHAR(3)    COMMENT 'lpad(CUST_TELEPHONE_NBR.EXCHANGE,3,0)'
   ,EXCHANGE_EXTENTION           VARCHAR(4)    COMMENT 'lpad(CUST_TELEPHONE_NBR.TN_LINE_NBR,4,0)'
   ,CUST_PHONE_STS               VARCHAR(2)    COMMENT 'Sourced from CUSTOMER_TELEPHONE.CUST_PHONE_STS  - Status code: AC - active DI- Disconnect, etc.'
   ,CUSTOMER_TN_TYPE             VARCHAR(6)    COMMENT 'Sourced from CUST_TELEPHONE_NBR.CUSTOMER_TN_TYPE_ID'
   ,SRVC_CLASS                   VARCHAR(2)    COMMENT 'Sourced from CUSTOMER_TELEPHONE.srvc_class'
   ,DTM_EFFTV                    TIMESTAMP     COMMENT 'Sourced from CUSTOMER_TELEPHONE.cust_tel_install_DT and CUST_TEL_INSTALL_TM Requires algorithm to get gregorian date-see next to_date(decode(b.cust_tel_install_DT + 19000000,19000000,19000101,b.cust_tel_install_DT + 19000000)|| ||to_char(lpad(b.CUST_TEL_INSTALL_TM,6,0)),yyyymmdd hh24miss)Will skip record is cust_tel_install_DT = 0. cust_tel_install_DT  should be greater than zero(0)'
   ,DTM_EXPIRED                  TIMESTAMP     COMMENT 'Sourced from CUSTOMER_TELEPHONE.cust_tel_DISC_DT and CUST_TEL_DISC_TM when cust_tel_DISC_DT is greater than zero to_date(decode(b.cust_tel_DISC_DT + 19000000,19000000,29991231,b.cust_tel_DISC_DT + 19000000)|| ||to_char(lpad(b.CUST_TEL_DISC_TM,6,0)),yyyymmdd hh24miss)Populate with 12/31/2999 when cust_tel_DISC_DT is equal zero(0)'
   ,CUST_TEL_INSTALL_DT          INT           COMMENT 'Install date - Sourced from CUSTOMER_TELEPHONE.cust_tel_install_DT'
   ,CUST_TEL_DISC_DT             INT           COMMENT 'Disconnect date -Sourced from CUSTOMER_TELEPHONE .cust_tel_DISC_DT'
   ,CUST_TEL_INSTALL_TM          INT           COMMENT 'Install time (hh24miss) - Sourced from CUSTOMER_TELEPHONE.cust_tel_install_DT'
   ,CUST_TEL_DISC_TM             INT           COMMENT 'disconnect time((hh24miss) -Sourced from CUSTOMER_TELEPHONE .cust_tel_DISC_TM'
   ,DTM_TEL_INSTALL              TIMESTAMP     COMMENT 'source from CUST_TEL_INSTALL_DT and  CUST_TEL_INSTALL_TM:to_date(decode(b.cust_tel_install_DT + 19000000,19000000,19000101,b.cust_tel_install_DT + 19000000)|| ||to_char(lpad(b.CUST_TEL_INSTALL_TM,6,0)),yyyymmdd hh24miss) dtm_tel_install If source data is zero 0 keep this null'
   ,DTM_TEL_DISCONNECT           TIMESTAMP     COMMENT 'source from CUST_TEL_DISC_DT and  CUST_TEL_DISC_TM:to_date(decode(b.cust_tel_DISC_DT + 19000000,19000000,29991231,b.cust_tel_DISC_DT + 19000000)|| ||to_char(lpad(b.CUST_TEL_DISC_TM,6,0)),yyyymmdd hh24miss) dtm_tel_disconnect If source data is zero 0 keep this null'
   ,DTM_CREATED                  TIMESTAMP     COMMENT 'Datetime record created in this table'
   ,DTM_LAST_MODIFIED            TIMESTAMP     COMMENT 'Datetime record created or updated'
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################