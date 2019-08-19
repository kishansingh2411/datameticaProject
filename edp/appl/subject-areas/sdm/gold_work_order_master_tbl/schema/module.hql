--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Search_engines table at Incoming layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2015
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
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
CHANGE_TYPE					 STRING      COMMENT '',
          WORK_ORDER_KEY                 BIGINT      COMMENT '',
          SITE_ID                        INT      COMMENT '',
          WORK_ORDER_NBR                 INT      COMMENT '',
          CHECK_IN_STATUS                STRING      COMMENT '',
          DATE_ENTERED                   INT      COMMENT '',
          TIME_ENTERED                   INT      COMMENT '',
          CUSTOMER_KEY                   BIGINT      COMMENT '',
          ACCT_NBR                       INT      COMMENT '',
          SALESMAN_NBR                   INT      COMMENT '',
          POOL                           STRING      COMMENT '',
          W_O_COMMENT_LINE_01            STRING      COMMENT '',
          W_O_COMMENT_LINE_02            STRING      COMMENT '',
          W_O_COMMENT_LINE_03            STRING      COMMENT '',
          REQUIRED_POINTS_CABLE_SERVICE  INT      COMMENT '',
          REQUIRED_TOTAL_POINTS          INT      COMMENT '',
          INSTALL_COMPLETION_DT          INT      COMMENT '',
          DEPOSIT_DOLLARS                DECIMAL(9,2)      COMMENT '',
          TOTAL_INSTALL_DOLLARS          DECIMAL(9,2)      COMMENT '',
          COD_DOLLARS                    DECIMAL(9,2)      COMMENT '',
          TOTAL_MONTHLY_SERVICE          DECIMAL(9,2)      COMMENT '',
          TOTAL_SERVICE_ACCRUAL_AMOUNT   DECIMAL(9,2)      COMMENT '',
          COMPANY_NBR                    INT      COMMENT '',
          DIVISION_NBR                   INT      COMMENT '',
          FRANCHISE_NBR                  INT      COMMENT '',
          PRIMARY_LOCATOR_CD             STRING      COMMENT '',
          SECONDARY_LOCATOR_CD           STRING      COMMENT '',
          PRORATED_AMOUNT                DECIMAL(9,2)      COMMENT '',
          SALES_DT                       INT      COMMENT '',
          REASON_NBR                     STRING      COMMENT '',
          LAST_CHANGE_USER_ID            STRING      COMMENT '',
          LAST_CHANGE_DT                 INT      COMMENT '',
          PRIMARY_FINDING_CD             STRING      COMMENT '',
          PRIMARY_SOLUTION_CD            STRING      COMMENT '',
          ALT_FINDING_CD_01              STRING      COMMENT '',
          ALT_SOLUTION_CD_01             STRING      COMMENT '',
          ALT_FINDING_CD_02              STRING      COMMENT '',
          ALT_SOLUTION_CD_02             STRING      COMMENT '',
          CHECK_IN_DEPOSIT_DOLLARS       DECIMAL(9,2)      COMMENT '',
          DISPATCH_SPECIAL_COMMENTS      STRING      COMMENT '',
          Q_CODE                         STRING      COMMENT '',
          W_O_STATUS                     STRING      COMMENT '',
          W_O_JOB_NBR                    INT      COMMENT '',
          W_O_TYPE                       STRING      COMMENT '',
          ASSIGNED_INSTALLER             INT      COMMENT '',
          INSTALLER_TRANSFER_FROM        INT      COMMENT '',
          ASSIGNED_TM                    INT      COMMENT '',
          CUSTOMER_ADDRESS               STRING      COMMENT '',
          PRIORITY                       STRING      COMMENT '',
          APARTMENT                      STRING      COMMENT '',
          OFFICE_ONLY_FLG                STRING      COMMENT '',
          ASSIGNED_DT                    INT      COMMENT '',
          COMPLETION_TM                  INT      COMMENT '',
          EMPLOYEE_TYPE_CD               STRING      COMMENT '',
          CUSTOMER_STATUS_CD             STRING      COMMENT '',
          INSTALL_OVERRIDE_CD            STRING      COMMENT '',
          TAG_NBR                        STRING      COMMENT '',
          START_BILLING_DT               INT      COMMENT '',
          CHECK_IN_DT                    INT      COMMENT '',
          CHECK_IN_USER_ID               STRING      COMMENT '',
          COD_FLG                        STRING      COMMENT '',
          BILL_TYPE_CD                   STRING      COMMENT '',
          DISC_MAJOR_CD                  STRING      COMMENT '',
          DEPOSIT_OVERRIDE_CD            STRING      COMMENT '',
          NBR_OF_PRIMARY_PAY_FROM_COMM   INT      COMMENT '',
          NBR_OF_PRIMARY_PAY_TO_COMM     INT      COMMENT '',
          NBR_OF_ADDL_PAY_FROM_COMM      INT      COMMENT '',
          BIGINT_OF_ADDL_PAY_TO_COMM     INT      COMMENT '',
          APARTMENT_RESIDENCE_CD         STRING      COMMENT '',
          PREVIOUS_CUSTOMER_FLG          STRING      COMMENT '',
          DWELLING_TYPE                  STRING      COMMENT '',
          USER_ID                        STRING      COMMENT '',
          HEADEND                        STRING      COMMENT '',
          CONTROL_EQUIPMENT_BALANCE      INT      COMMENT '',
          RANGE_NBR                      BIGINT      COMMENT '',
          BILLING_RANGE_NBR              BIGINT      COMMENT '',
          OLD_RANGE_NBR                  BIGINT      COMMENT '',
          OLD_BILLING_RANGE_NBR          BIGINT      COMMENT '',
          BIGINT_OF_RESCHEDULES          INT      COMMENT '',
          BALANCE_TO_PENDING_ORDER       STRING      COMMENT '',
          FUTURE_USE_01_WO               STRING      COMMENT '',
          FUTURE_USE_02_WO               STRING      COMMENT '',
          FUTURE_USE_03_WO               STRING      COMMENT '',
          POINTS_OVERRIDE_FLG            STRING      COMMENT '',
          PRORATES_PROCESSED             STRING      COMMENT '',
          PURGE_FLAG_DEPOSIT_REFUND      STRING      COMMENT '',
          FUTURE_USE_05_WO               STRING      COMMENT '',
          PURGE_FLG                      STRING      COMMENT '',
          W_O_CATEGORY                   STRING      COMMENT '',
          ALT_FINDING_CD_03              STRING      COMMENT '',
          ALT_SOLUTION_CD_03             STRING      COMMENT '',
          ALT_SOLUTION_CD_04             STRING      COMMENT '',
          ALT_SOLUTION_CD_05             STRING      COMMENT '',
          ALT_SOLUTION_CD_06             STRING      COMMENT '',
          ALT_SOLUTION_CD_07             STRING      COMMENT '',
          PROBLEM_CD_01                  STRING      COMMENT '',
          PROBLEM_CD_02                  STRING      COMMENT '',
          PROBLEM_CD_03                  STRING      COMMENT '',
          PROBLEM_CD_04                  STRING      COMMENT '',
          PROBLEM_CD_05                  STRING      COMMENT '',
          SOFT_DISC_FLG                  STRING      COMMENT '',
          REQUEST_SCHEDULE_DT            STRING      COMMENT '',
          CANCEL_CD                      STRING      COMMENT '',
          SCHEDULE_DT                    INT      COMMENT '',
          ORIGINAL_SCHEDULE_DT           INT      COMMENT '',
          W_O_PRINTED_FLG                STRING      COMMENT '',
          SYSTEM_INSTALL_COMP_DT         INT      COMMENT '',
          SYSTEM_COMPLETION_TM           INT      COMMENT '',
          MONTHLY_DOLLARS                DECIMAL(9,2)      COMMENT '',
          TIME_SLOT                      STRING      COMMENT '',
          NODE                           STRING      COMMENT '',
          STOP_BILLING                   STRING      COMMENT '',
          JOB_SEQUENCE_NBR               STRING      COMMENT '',
          W_O_CLASS                      STRING      COMMENT '',
          CHECK_IN_TM                    INT      COMMENT '',
          STAGE_CD                       STRING      COMMENT '',
          CO_ID                          STRING      COMMENT '',
          NETWORK_ELEMENT_ID             STRING      COMMENT '',
          ARRIVAL_DT                     INT      COMMENT '',
          APPOINTMENT_CD                 STRING      COMMENT '',
          ARRIVAL_TM                     INT      COMMENT '',
          COMMITMENT_DT                  INT      COMMENT '',
          COMMITMENT_TM                  INT      COMMENT '',
          OUT_OF_SERVICE_FLG             STRING      COMMENT '',
          PLANT_STUDY_CD                 STRING      COMMENT '',
          FAULT_CD                       STRING      COMMENT '',
          RESTORAL_DT                    INT      COMMENT '',
          RESTORAL_TM                    INT      COMMENT '',
          CUSTOMER_NOTIFIED_FLG          STRING      COMMENT '',
          REQUESTED_SCHEDULE_DT          INT      COMMENT '',
          REQUESTED_SCHEDULE_TM          INT      COMMENT '',
          GRID_ID                        STRING      COMMENT '',
          TELEPHONY_CANCEL_CD            STRING      COMMENT '',
          CAMPAIGN_GROUP_CD              INT      COMMENT '',
          OVERRIDE_BILL_DAY              INT      COMMENT '',
          DISPATCH_USER_ID               STRING      COMMENT '',
          SCHEDULE_CD_1                  STRING      COMMENT '',
          SCHEDULE_CD_2                  STRING      COMMENT '',
          SCHEDULE_CD_3                  STRING      COMMENT '',
          SOURCE_CD                      STRING      COMMENT '',
          OUTAGE_CAUSE_CD                STRING      COMMENT '',
          HOUSE_NBR                      INT      COMMENT '',
          ORDER_ENTRY_GROUP_NBR          INT      COMMENT '',
          CALL_CENTER_GROUPING_CD        STRING      COMMENT '',
          CALL_CD                        INT      COMMENT '',
          BILLED_THRU_DATE_CHECK_IN      INT      COMMENT '',
          PROMOTION_GROUP_CD             INT      COMMENT '',
          CONTRACT_PRICING_ID            STRING      COMMENT '',
          CAMPAIGN_CD                    STRING      COMMENT '',
          PACKAGE_CD                     STRING      COMMENT '',
          CREATED                        STRING      COMMENT '',
          UPDATED                        STRING      COMMENT '',
          finding_desc                   STRING      COMMENT '',
          soulition_desc                 STRING      COMMENT '',
          problem_desc_01                STRING      COMMENT '',
          problem_desc_02                STRING      COMMENT '',
          sales_name                     STRING      COMMENT '',
          tech_name                      STRING      COMMENT '',
          entered_timestamp              STRING      COMMENT '',
          schedule_timestamp             STRING      COMMENT '',
          complete_timestamp             STRING      COMMENT '',
          checkin_timestamp              STRING      COMMENT ''
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################