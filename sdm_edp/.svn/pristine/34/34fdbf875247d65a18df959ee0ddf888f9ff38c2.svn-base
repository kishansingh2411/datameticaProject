--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_cdr_resi_tbl table at Gold layer
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
  CALLDATE                        INT             COMMENT '',
  CALLTIME                        INT             COMMENT '',
  ANIORIG                         BIGINT          COMMENT '',
  ANIACCNT                        BIGINT          COMMENT '',
  ACCTNUM                         INT             COMMENT '',
  SITEID                          SMALLINT        COMMENT '',
  DNIS                            BIGINT          COMMENT '',
  RESCOM                          STRING          COMMENT '',
  IVR_LANGUAGE                    TINYINT         COMMENT '',
  LOCATOR_SN                      BIGINT          COMMENT '',
  REGION                          STRING          COMMENT '',
  SUBREGION                       STRING          COMMENT '',
  DEPT                            STRING          COMMENT '',
  CALL_TYPE                       STRING          COMMENT '',
  CUSTOMERVERIFIEDFORAUTOMATION   STRING          COMMENT '',
  PAYMENT_BLOCKED                 STRING          COMMENT '',
  VIP                             STRING          COMMENT '',
  COLLECTIONS                     STRING          COMMENT '',
  AUTOMATION_FAIL                 STRING          COMMENT '',
  AUTO_NAME                       STRING          COMMENT '',
  REPEAT_CALLER                   STRING          COMMENT '',
  APPOINTMENT                     STRING          COMMENT '',
  LINES_OF_BUSINESS               STRING          COMMENT '',
  TRUCK_ROLL_REQUIRED             STRING          COMMENT '',
  INFO_MSG_PLAYED                 STRING          COMMENT '',
  OUTAGE_MSG_PLAYED               STRING          COMMENT '',
  CALL_ID                         STRING          COMMENT '',
  ZIPCODE                         STRING          COMMENT '',
  REQUESTTYPE                     STRING          COMMENT '',
  DISPSTATUS                      STRING          COMMENT '',
  DISPCODE                        STRING          COMMENT '',
  DISPOSPKCSR                     STRING          COMMENT '',
  EOS_UNIT                        INT             COMMENT '',
  EOS_UNIT_LINE                   INT             COMMENT '',
  DURATION                        INT             COMMENT '',
  APN                             BIGINT          COMMENT '',
  APP_NAME                        STRING          COMMENT '',
  ENTERED                         STRING          COMMENT '',
  ZIP_CODE                        INT             COMMENT '',
  LANG                            STRING          COMMENT '',
  CUSTOMER                        STRING          COMMENT '',
  SERVICE_TYPE                    STRING          COMMENT '',
  EXIT_POINT                      STRING          COMMENT '',
  TRANSFER_TERM                   BIGINT          COMMENT '',
  TRANSFER_ATTEMPT                STRING          COMMENT '',
  TRANSFER_RESULT                 STRING          COMMENT '',
  P2P_MODULE_ENTERED              STRING          COMMENT '',
  P2P_SUCCESS                     STRING          COMMENT '',
  P2PSD_MODULE_ENTERED            STRING          COMMENT '',
  P2PSD_SUCCESS                   STRING          COMMENT '',
  CC_PMT                          STRING          COMMENT '',
  CC_PMT_SUCCESS                  STRING          COMMENT '',
  CC_PMT_AMOUNT                   DECIMAL(10,2)   COMMENT '',
  PAY_BY_CHECK                    STRING          COMMENT '',
  PAY_BY_CHECK_SUCCESS            STRING          COMMENT '',
  PAY_BY_CHECK_AMOUNT             DECIMAL(10,2)   COMMENT '',
  TECH_OUTAGE                     STRING          COMMENT '',
  TECH_OUTAGE_SUCCESS             STRING          COMMENT '',
  TECHPULSE_TRUCK_ROLL_SCHEDULED  STRING          COMMENT '',
  TECHPULSE_TRK_ROLL_SCHED_SUCC   STRING          COMMENT '',
  TECHPULSE_TRUCK_ROLL_NEW        STRING          COMMENT '',
  TECHPULSE_TRK_ROLL_NEW_SUCC     STRING          COMMENT '',
  SAFEGUARD                       STRING          COMMENT '',
  SAFEGUARD_ORDERED               STRING          COMMENT '',
  PPV                             STRING          COMMENT '',
  PPV_SUCCESS                     STRING          COMMENT '',
  PPV_EVENT_ORDERED               STRING          COMMENT '',
  APPT_SCHEDULE                   STRING          COMMENT '',
  APPT_SCHEDULE_SUCCESS           STRING          COMMENT '',
  APPT_CONFIRMED                  STRING          COMMENT '',
  APPT_NEW                        STRING          COMMENT '',
  APPT_RESCHED                    STRING          COMMENT '',
  APPT_CANCEL                     STRING          COMMENT '',
  APPT_WHERE_IS_TECH              STRING          COMMENT '',
  PREDICTIVE_INTENT_TRIGGERED     STRING          COMMENT '',
  CALL_PATHING                    STRING          COMMENT ''
)
PARTITIONED BY (P_YYYYMMDD string)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################