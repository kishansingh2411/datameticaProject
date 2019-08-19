--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_rsdvr_schedule_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 05/05/2016
--#   Log File    : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/05/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
ID_SERVICE_REC                 INT          COMMENT '',
HOME_ID                         VARCHAR(255)    COMMENT '',
ACCOUNT_NUMBER             VARCHAR(255)  COMMENT '',
ID_SERVICE_TYPE                 INT    COMMENT '',
ID_SYS_INFO                     INT    COMMENT '',
ID_STATUS                     INT    COMMENT '',
CH_HOME_PH                     VARCHAR(20)  COMMENT '',
CH_WORK_PH                     VARCHAR(20)  COMMENT '',
CH_EMAIL_ADD                 VARCHAR(30)  COMMENT '',
CH_USERNAME                  VARCHAR(10)  COMMENT '',
CH_PASSWD                       VARCHAR(10)  COMMENT '',
DT_START_DATE                 DATE   COMMENT '',
DT_END_DATE                     DATE   COMMENT '',
DT_LAS_MOD_DATE               DATE      COMMENT '',
CH_HOMEAREA_CD                 VARCHAR(3)  COMMENT '',
CH_RATE                         DECIMAL(7,2) COMMENT '',
CH_BDATE                     DATE      COMMENT '',
CH_BILLTHRUDATE                 DATE      COMMENT '',
CH_CYCLE                     VARCHAR(1)  COMMENT '',
CH_DRSN                         VARCHAR(2)  COMMENT '',
CH_HOLD                         INT          COMMENT '',
CH_COLLECT_STATUS             VARCHAR(1)  COMMENT '',
CH_ACCT_STATUS_QUALIFIER     VARCHAR(20)  COMMENT '',
DT_ACCT_STATUS_QUALIFIER_DATE DATE      COMMENT '',
DT_STATUS_DATE                 DATE      COMMENT '',
OP_TYPE                         VARCHAR(1)  COMMENT '',
LOAD_DATE                     DATE      COMMENT '',
CUST_SERVICES_DELTA_SEQ         INT          COMMENT ''
    )
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################