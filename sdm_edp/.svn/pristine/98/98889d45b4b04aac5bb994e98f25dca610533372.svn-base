--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_cdr_resi_tbl table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/30/2017
--#   Log File    : .../log/ivr/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/ivr/${job_name}.log
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

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_cdr_resi_table}
  PARTITION(P_YYYYMMDD)
SELECT
    (CALLDATE + 19000000) as CALLDATE,
    CALLTIME,
    ANIORIG,
    ANIACCNT,
    ACCTNUM,
    SITEID,
    DNIS,
    RESCOM,
    IVR_LANGUAGE,
    LOCATOR_SN,
    REGION,
    SUBREGION,
    DEPT,
    CALL_TYPE,
    CUSTOMERVERIFIEDFORAUTOMATION,
    PAYMENT_BLOCKED,
    VIP,
    COLLECTIONS,
    AUTOMATION_FAIL,
    AUTO_NAME,
    REPEAT_CALLER,
    APPOINTMENT,
    LINES_OF_BUSINESS,
    TRUCK_ROLL_REQUIRED,
    INFO_MSG_PLAYED,
    OUTAGE_MSG_PLAYED,
    CALL_ID,
    ZIPCODE,
    REQUESTTYPE,
    DISPSTATUS,
    DISPCODE,
    DISPOSPKCSR,
    EOS_UNIT,
    EOS_UNIT_LINE,
    DURATION,
    APN,
    APP_NAME,
    ENTERED,
    ZIP_CODE,
    LANG,
    CUSTOMER,
    SERVICE_TYPE,
    EXIT_POINT,
    TRANSFER_TERM,
    TRANSFER_ATTEMPT,
    TRANSFER_RESULT,
    P2P_MODULE_ENTERED,
    P2P_SUCCESS,
    P2PSD_MODULE_ENTERED,
    P2PSD_SUCCESS,
    CC_PMT,
    CC_PMT_SUCCESS,
    CC_PMT_AMOUNT,
    PAY_BY_CHECK,
    PAY_BY_CHECK_SUCCESS,
    PAY_BY_CHECK_AMOUNT,
    TECH_OUTAGE,
    TECH_OUTAGE_SUCCESS,
    TECHPULSE_TRUCK_ROLL_SCHEDULED,
    TECHPULSE_TRK_ROLL_SCHED_SUCC,
    TECHPULSE_TRUCK_ROLL_NEW,
    TECHPULSE_TRK_ROLL_NEW_SUCC,
    SAFEGUARD,
    SAFEGUARD_ORDERED,
    PPV,
    PPV_SUCCESS,
    PPV_EVENT_ORDERED,
    APPT_SCHEDULE,
    APPT_SCHEDULE_SUCCESS,
    APPT_CONFIRMED,
    APPT_NEW,
    APPT_RESCHED,
    APPT_CANCEL,
    APPT_WHERE_IS_TECH,
    PREDICTIVE_INTENT_TRIGGERED,
   CALL_PATHING,
   P_YYYYMMDD
FROM  ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_cdr_resi_table}
WHERE P_YYYYMMDD='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################