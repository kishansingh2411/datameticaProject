--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(gold_h_optimum_user_tbl)
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

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
    H_OPTIMUM_USER_ID        INT           COMMENT '',
    OPTIMUM_USER_ID          INT           COMMENT 'This is a system assigned non-intrinsic number which uniquely identifies an optimum id in this database/schema',
    CUSTOMER_ACCOUNT_ID      INT           COMMENT 'Foreign key to the CVCDCP.CDRMGR.CUSTOMER_ACCOUNT table.  This is a system assigned non-intrinsic number which uniquely identifies a Cabledata account in the CVCDRP/CDRMGR database/schema',
    DTM_EFFTV                TIMESTAMP     COMMENT 'Date/time that the record becomes effective/active.  On an insert of the record, this is sourced from the delta records LOAD_DATE and does not change',
    DTM_EXPIRED              TIMESTAMP     COMMENT 'Date/time of when the record expires.  A date/time of December 31, 2999 equates to and end date that has not been established. This date is a standard established for ease of programming in using the between verb for queries against the record.  When an update of this record occurs and the OP_TYPE = D, a logical delete of the record will occur.  At this time, this column will be updated with the LOAD_DATE from the record the data was sourced',
    DTM_CREATED              TIMESTAMP     COMMENT 'date/time the record was inserted into the table.  Sourced from SYSDATE',
    CREATED_BY_PROCESS       STRING        COMMENT 'Sourced from: the name of the Informatica workflow',
    SOURCED_FROM_SYSTEM      STRING        COMMENT 'Sourced from: ENGCUP.AMS',
    LAST_UPDATED_BY_PROCESS  STRING        COMMENT 'Sourced from: the name of the Informatica workflow (NULL when the row is inserted)',
    LAST_UPDATED_FROM_SYSTEM STRING        COMMENT 'Sourced from: ENGCUP.AMS. (NULL when the row is inserted)',
    DTM_LAST_UPDATED         TIMESTAMP     COMMENT 'Date and time when the record was last updated.  This column is NULL when the record is inserted.  When updated, it is sourced from SYSDATE',
    DTM_LAST_MODIFIED        TIMESTAMP     COMMENT 'Date and time when the record was last modified.  On an insert, this is equal to the dtm_created.  On an update, this is equal to the dtm_last_updated.  In either case, it is sourced from SYSDATE.',
    ID_CUST                  INT           COMMENT 'Unique Identification of a customer in the CCIP.CUST_IDX database/schema.  Sourced from ENGCUP.AMS.HOUSE_HOLD_USERS.ID_CUST',
    ID_SERVICE_REC           INT           COMMENT 'Unique identification of a customer account in the CCIP.CUST_IDX.CUST_SERVICES database/schema/table.  Sourced from ENGCUP.AMS.HOUSE_HOLD_USERS.id_service_rec',
    ID_USER                  INT           COMMENT 'Unique identification in ENGCUP.AMS.HOUSE_HOLD_USERS and ENGCUP.AMS.USER_PROFILES.  Used to uniquely identify an optimum_id (CH_LOGIN) in the Provisioning ENGCUP.AMS database.schema.  Sourced from: CVCDCP.CDCUSR.USER_PROFILES.ID_USER',
    CORP                     INT           COMMENT 'Generally accepted identificaiton of a geographic region of customer accounts from the Cabledata application.  Sourced from: cvcdcp.cdcusr.house_hold_users_delta.substr(ch_account_number,1,5).  If the CH_ACCOUNT_NUMBER is null, the corp will derived in one of two ways.  (1) if there are no active CVCDCP.CDCUSR.CUST_SERVICES_LAST_REC records for the CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.ID_CUST, the oldest (chronilogically) CH_ACCOUNT_NUMBER for the ID_CUST will be used to get the corp.  (2)  if there are active CVCDCP.CDCUSR.CUST_SERVICES_LAST_REC records for the CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.ID_CUST, the oldest (chronilogically) CH_ACCOUNT_NUMBER for the ID_CUST will be used to get the corp',
    DWELLING_NBR             STRING        COMMENT 'Generally accepted identificaiton of a house within a corp of a customer account from the Cabledata application.  Sourced from: cvcdcp.cdcusr.house_hold_users_delta.substr(ch_account_number,6,6).  If the CH_ACCOUNT_NUMBER is null, the corp will derived in one of two ways. (1) if there are no active CVCDCP.CDCUSR.CUST_SERVICES_LAST_REC records for the CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.ID_CUST, the oldest (chronilogically) CH_ACCOUNT_NUMBER for the ID_CUST will be used to get the house.  (2)  if there are active CVCDCP.CDCUSR.CUST_SERVICES_LAST_REC records for the CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.ID_CUST, the oldest (chronilogically) CH_ACCOUNT_NUMBER for the ID_CUST will be used to get the house',
    CUST                     STRING        COMMENT 'Generally accepted identificaiton of an account within a corp/house from the Cabledata application.  Sourced from: cvcdcp.cdcusr.house_hold_users_delta.substr(ch_account_number,13,2).  If the CH_ACCOUNT_NUMBER is null, the corp will derived in one of two ways.  (1) if there are no active CVCDCP.CDCUSR.CUST_SERVICES_LAST_REC records for the CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.ID_CUST, the oldest (chronilogically) CH_ACCOUNT_NUMBER for the ID_CUST will be used to get the cust.  (2)  if there are active CVCDCP.CDCUSR.CUST_SERVICES_LAST_REC records for the CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.ID_CUST, the oldest (chronilogically) CH_ACCOUNT_NUMBER for the ID_CUST will be used to get the cust',
    DTM_START_DATE           DATE          COMMENT 'date/time an optimum id became effective within the ENGCUP.AMS database.schema.  Sourced from: CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.DT_START_DATE',
    DTM_END_DATE             DATE          COMMENT 'date/time an optimum id expired within the ENGCUP.AMS database.schema.  Sourced from: CVCDCP.CDCUSR.HOUSE_HOLD_USERS_DELTA.DT_END_DATE.',
    EMAIL_IND                STRING        COMMENT 'Specifies if the optimum_id has email.  Sourced from: CDCMGR.USER_PROFILES_DELTA.EMAIL_FLAG.',
    ID_DOMAIN                INT           COMMENT 'Specifies the domain of the email address related to the OPTIMUM_ID.  If source is NULL, set to 2 (per Agreement with KOM).  Sourced from: CDCMGR.USER_PROFILES_DELTA.ID_DOMAIN',
    OPTIMUM_ID               STRING        COMMENT 'Identification of an optimum id in the ENGCUP.AMS database/schema.  This is NOT always unique Sourced from lower(ENGCUP.AMS.USER_PROFILES.CH_LOGIN)',
    MIXED_CASE_OPTIMUM_ID    STRING        COMMENT 'optimum id in the ENGCUP.AMS database/schema.  Sourced from ENGCUP.AMS.USER_PROFILES.CH_LOGIN in whatever case it is in ENGCUP',
    ID_USER_TYPE             INT           COMMENT 'Specifies if the OPTIMUM_ID used to manually log-in (or used to registered the device wifi automatic login) is a primary, secondary or temporary ID.  Domain: 1 =Primary, 2 =Secondary, 3=Temporary.  Source from: CVCDCP.CDCMGR.USER_PROFILES_DELTA.ID_USER_TYPE',
    USER_TYPE_DESCR          STRING        COMMENT 'The decode (label) given to the ID_USER_TYPE.  Domain: 1=Primary, 2=Secondary, 3=Temporary.  Source from: CVCDCP.CDCMGR.USER_PROFILES_DELTA.ID_USER_TYPE',
    SOURCE_APPL              STRING        COMMENT 'Name of the Application used to create the Optimum ID.  Source from: CVCDCP.CDCMGR.USER_PROFILES_DELTA.SOURCE_APP'
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################