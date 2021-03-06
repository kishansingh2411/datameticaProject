--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create incoming_work_order_master table at Incoming layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 02/10/2017
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
--#    1.0     DataMetica Team          02/10/2017       Initial version
--#
--#
--#####################################################################################################################

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
  LAST_CHANGE                    TIMESTAMP,
  CHANGE_TYPE                    CHAR(1),
  WORK_ORDER_KEY                 BIGINT,
  SITE_ID                        INT,
  WORK_ORDER_NBR                 INT,
  CHECK_IN_STATUS                STRING,
  DATE_ENTERED                   INT,
  TIME_ENTERED                   INT,
  CUSTOMER_KEY                   BIGINT,
  ACCT_NBR                       INT,
  SALESMAN_NBR                   INT,
  POOL                           STRING,
  W_O_COMMENT_LINE_01            STRING,
  W_O_COMMENT_LINE_02            STRING,
  W_O_COMMENT_LINE_03            STRING,
  REQUIRED_POINTS_CABLE_SERVICE  INT,
  REQUIRED_TOTAL_POINTS          INT,
  INSTALL_COMPLETION_DT          INT,
  DEPOSIT_DOLLARS                DECIMAL(9,2),
  TOTAL_INSTALL_DOLLARS          DECIMAL(9,2),
  COD_DOLLARS                    DECIMAL(9,2),
  TOTAL_MONTHLY_SERVICE          DECIMAL(9,2),
  TOTAL_SERVICE_ACCRUAL_AMOUNT   DECIMAL(9,2),
  COMPANY_NBR                    INT,
  DIVISION_NBR                   INT,
  FRANCHISE_NBR                  INT,
  PRIMARY_LOCATOR_CD             STRING,
  SECONDARY_LOCATOR_CD           STRING,
  PRORATED_AMOUNT                DECIMAL(9,2),
  SALES_DT                       INT,
  REASON_NBR                     STRING,
  LAST_CHANGE_USER_ID            STRING,
  LAST_CHANGE_DT                 INT,
  PRIMARY_FINDING_CD             STRING,
  PRIMARY_SOLUTION_CD            STRING,
  ALT_FINDING_CD_01              STRING,
  ALT_SOLUTION_CD_01             STRING,
  ALT_FINDING_CD_02              STRING,
  ALT_SOLUTION_CD_02             STRING,
  CHECK_IN_DEPOSIT_DOLLARS       DECIMAL(9,2),
  DISPATCH_SPECIAL_COMMENTS      STRING,
  Q_CODE                         STRING,
  W_O_STATUS                     STRING,
  W_O_JOB_NBR                    INT,
  W_O_TYPE                       STRING,
  ASSIGNED_INSTALLER             INT,
  INSTALLER_TRANSFER_FROM        INT,
  ASSIGNED_TM                    INT,
  CUSTOMER_ADDRESS               STRING,
  PRIORITY                       STRING,
  APARTMENT                      STRING,
  OFFICE_ONLY_FLG                STRING,
  ASSIGNED_DT                    INT,
  COMPLETION_TM                  INT,
  EMPLOYEE_TYPE_CD               STRING,
  CUSTOMER_STATUS_CD             STRING,
  INSTALL_OVERRIDE_CD            STRING,
  TAG_NBR                        STRING,
  START_BILLING_DT               INT,
  CHECK_IN_DT                    INT,
  CHECK_IN_USER_ID               STRING,
  COD_FLG                        STRING,
  BILL_TYPE_CD                   STRING,
  DISC_MAJOR_CD                  STRING,
  DEPOSIT_OVERRIDE_CD            STRING,
  NBR_OF_PRIMARY_PAY_FROM_COMM   INT,
  NBR_OF_PRIMARY_PAY_TO_COMM     INT,
  NBR_OF_ADDL_PAY_FROM_COMM      INT,
  NUMBER_OF_ADDL_PAY_TO_COMM     INT,
  APARTMENT_RESIDENCE_CD         STRING,
  PREVIOUS_CUSTOMER_FLG          STRING,
  DWELLING_TYPE                  STRING,
  USER_ID                        STRING,
  HEADEND                        STRING,
  CONTROL_EQUIPMENT_BALANCE      INT,
  RANGE_NBR                      BIGINT,
  BILLING_RANGE_NBR              BIGINT,
  OLD_RANGE_NBR                  BIGINT,
  OLD_BILLING_RANGE_NBR          BIGINT,
  NUMBER_OF_RESCHEDULES          INT,
  BALANCE_TO_PENDING_ORDER       STRING,
  FUTURE_USE_01_WO               STRING,
  FUTURE_USE_02_WO               STRING,
  FUTURE_USE_03_WO               STRING,
  POINTS_OVERRIDE_FLG            STRING,
  PRORATES_PROCESSED             STRING,
  PURGE_FLAG_DEPOSIT_REFUND      STRING,
  FUTURE_USE_05_WO               STRING,
  PURGE_FLG                      STRING,
  W_O_CATEGORY                   STRING,
  ALT_FINDING_CD_03              STRING,
  ALT_SOLUTION_CD_03             STRING,
  ALT_SOLUTION_CD_04             STRING,
  ALT_SOLUTION_CD_05             STRING,
  ALT_SOLUTION_CD_06             STRING,
  ALT_SOLUTION_CD_07             STRING,
  PROBLEM_CD_01                  STRING,
  PROBLEM_CD_02                  STRING,
  PROBLEM_CD_03                  STRING,
  PROBLEM_CD_04                  STRING,
  PROBLEM_CD_05                  STRING,
  SOFT_DISC_FLG                  STRING,
  REQUEST_SCHEDULE_DT            STRING,
  CANCEL_CD                      STRING,
  SCHEDULE_DT                    INT,
  ORIGINAL_SCHEDULE_DT           INT,
  W_O_PRINTED_FLG                STRING,
  SYSTEM_INSTALL_COMP_DT         INT,
  SYSTEM_COMPLETION_TM           INT,
  MONTHLY_DOLLARS                DECIMAL(9,2),
  TIME_SLOT                      STRING,
  NODE                           STRING,
  STOP_BILLING                   STRING,
  JOB_SEQUENCE_NBR               STRING,
  W_O_CLASS                      STRING,
  CHECK_IN_TM                    INT,
  STAGE_CD                       STRING,
  CO_ID                          STRING,
  NETWORK_ELEMENT_ID             STRING,
  ARRIVAL_DT                     INT,
  APPOINTMENT_CD                 STRING,
  ARRIVAL_TM                     INT,
  COMMITMENT_DT                  INT,
  COMMITMENT_TM                  INT,
  OUT_OF_SERVICE_FLG             STRING,
  PLANT_STUDY_CD                 STRING,
  FAULT_CD                       STRING,
  RESTORAL_DT                    INT,
  RESTORAL_TM                    INT,
  CUSTOMER_NOTIFIED_FLG          STRING,
  REQUESTED_SCHEDULE_DT          INT,
  REQUESTED_SCHEDULE_TM          INT,
  GRID_ID                        STRING,
  TELEPHONY_CANCEL_CD            STRING,
  CAMPAIGN_GROUP_CD              INT,
  OVERRIDE_BILL_DAY              INT,
  DISPATCH_USER_ID               STRING,
  SCHEDULE_CD_1                  STRING,
  SCHEDULE_CD_2                  STRING,
  SCHEDULE_CD_3                  STRING,
  SOURCE_CD                      STRING,
  OUTAGE_CAUSE_CD                STRING,
  HOUSE_NBR                      INT,
  ORDER_ENTRY_GROUP_NBR          INT,
  CALL_CENTER_GROUPING_CD        STRING,
  CALL_CD                        INT,
  BILLED_THRU_DATE_CHECK_IN      INT,
  PROMOTION_GROUP_CD             INT,
  CONTRACT_PRICING_ID            STRING,
  CAMPAIGN_CD                    STRING,
  PACKAGE_CD                     STRING,
  CREATED                        TIMESTAMP,
  UPDATED                        TIMESTAMP
)
PARTITIONED BY (P_YYYYMMDD  string) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################