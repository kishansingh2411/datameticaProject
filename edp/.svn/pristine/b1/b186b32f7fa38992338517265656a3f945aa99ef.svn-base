--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_cust_telephone_nbr)
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

CREATE EXTERNAL TABLE 
   ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   SITE_ID                        INT         COMMENT 'CUST_TELEPHONE_NBR.SITE_ID IS MCNROV'
   ,ACCT_NBR                      INT         COMMENT 'CUST_TELEPHONE_NBR.ACCT_NBR IS MCCNBR'
   ,CUSTOMER_KEY                  INT         COMMENT ''
   ,CUST_TN_SEQ                   INT         COMMENT 'CUST_TELEPHONE_NBR.CUST_TN_SEQ IS MCBLT0'
   ,TN_SRCE                       VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.TN_SRCE IS MCAVUW'
   ,CUST_TN_STS                   VARCHAR(2)  COMMENT 'CUST_TELEPHONE_NBR.CUST_TN_STS IS MCAVUX'
   ,TN_CHARACTER_STRING           VARCHAR(20) COMMENT 'CUST_TELEPHONE_NBR.TN_CHARACTER_STRING IS MCBJVF'
   ,TN_FORMAT_MASK                VARCHAR(3)  COMMENT 'CUST_TELEPHONE_NBR.TN_FORMAT_MASK IS MCBHSR'
   ,NPA_STD                       INT         COMMENT 'CUST_TELEPHONE_NBR.NPA_STD IS MCBLT1'
   ,TN_LINE_NBR                   INT         COMMENT 'CUST_TELEPHONE_NBR.TN_LINE_NBR IS MCBLT3'
   ,`EXCHANGE`                      INT         COMMENT 'CUST_TELEPHONE_NBR.EXCHANGE IS MCBLT2'
   ,TN_USER_DISPLAY_NM            VARCHAR(15) COMMENT 'CUST_TELEPHONE_NBR.TN_USER_DISPLAY_NM IS MCBJVG'
   ,DEFAULT_LINE_DIRECTION        VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.DEFAULT_LINE_DIRECTION IS MCAVUY'
   ,PORTING_USE                   VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.PORTING_USE IS MCAVUZ'
   ,USAGE_REPORTING_TYPE          VARCHAR(2)  COMMENT 'CUST_TELEPHONE_NBR.USAGE_REPORTING_TYPE IS MCBHSS'
   ,SEC_REPORTING_TYPE            VARCHAR(3)  COMMENT 'CUST_TELEPHONE_NBR.SEC_REPORTING_TYPE IS MCBHST'
   ,DISC_TREATMENT_ID             VARCHAR(4)  COMMENT 'CUST_TELEPHONE_NBR.DISC_TREATMENT_ID IS MCBHSU'
   ,TREATMENT_FORWARDING_TN       VARCHAR(12) COMMENT 'CUST_TELEPHONE_NBR.TREATMENT_FORWARDING_TN IS MCBJVH'
   ,TREATMENT_STS_DT              INT         COMMENT 'CUST_TELEPHONE_NBR.TREATMENT_STS_DT IS MCACJ7'
   ,TREATMENT_BEGIN_DT            INT         COMMENT 'CUST_TELEPHONE_NBR.TREATMENT_BEGIN_DT IS MCACJ8'
   ,TREATMENT_EXPIRATION_DT       INT         COMMENT 'CUST_TELEPHONE_NBR.TREATMENT_EXPIRATION_DT IS MCACJ9'
   ,FX_TN                         VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.FX_TN IS MCAVU1'
   ,EMS_LOC                       VARCHAR(60) COMMENT 'CUST_TELEPHONE_NBR.EMS_LOC IS MCBJVI'
   ,TN_EMS_CLASS_OF_SRVC          VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.TN_EMS_CLASS_OF_SRVC IS MCBHSV'
   ,TN_EMS_TYPE_OF_SRVC           VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.TN_EMS_TYPE_OF_SRVC IS MCBHSW'
   ,TN_EMS_ALSO_RINGS_AT_ADDR     VARCHAR(60) COMMENT 'CUST_TELEPHONE_NBR.TN_EMS_ALSO_RINGS_AT_ADDR IS MCBJVJ'
   ,ILEC_ACCT_NBR                 VARCHAR(20) COMMENT 'CUST_TELEPHONE_NBR.ILEC_ACCT_NBR IS MCBHSX'
   ,TRUNK_GRP_ID                  VARCHAR(4)  COMMENT 'CUST_TELEPHONE_NBR.TRUNK_GRP_ID IS MCBHSY'
   ,TRUNK_GRP_LRN                 VARCHAR(20) COMMENT 'CUST_TELEPHONE_NBR.TRUNK_GRP_LRN IS MCBJVK'
   ,TRUNK_GRP_CHANNEL             INT         COMMENT 'CUST_TELEPHONE_NBR.TRUNK_GRP_CHANNEL IS MCBLT4'
   ,TN_USE_TYPE                   VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.TN_USE_TYPE IS MCAWHZ'
   ,ILEC_ACCT_TN                  VARCHAR(16) COMMENT 'CUST_TELEPHONE_NBR.ILEC_ACCT_TN IS MCBHT1'
   ,PORT_FLG                      VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.PORT_FLG IS MCAVZF'
   ,ILEC_CUST_ACCT_CD             VARCHAR(3)  COMMENT 'CUST_TELEPHONE_NBR.ILEC_CUST_ACCT_CD IS MCBHV5'
   ,LSP_CHG_PROHIBIT              VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.LSP_CHG_PROHIBIT IS MCAVZG'
   ,BLOCKING_OPTIONS              VARCHAR(16) COMMENT 'CUST_TELEPHONE_NBR.BLOCKING_OPTIONS IS MCBHV6'
   ,MASTER_SUB_ACCT_USE           VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.MASTER_SUB_ACCT_USE IS MCAVUT'
   ,ROLL_UP_USAGE_TO_PILOT_TN     VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.ROLL_UP_USAGE_TO_PILOT_TN IS MCAWIE'
   ,INCLUDE_IN_STMT_SUMMARY       VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.INCLUDE_IN_STMT_SUMMARY IS MCAWH1'
   ,INCLUDE_IN_OCC_SUMMARY        VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.INCLUDE_IN_OCC_SUMMARY IS MCAWH2'
   ,LANGUAGE_IND                  VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.LANGUAGE_IND IS MCBHZ3'
   ,NON_PUBLISHED_CD              VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.NON_PUBLISHED_CD IS MCSUFR'
   ,DISC_MAJOR_CD                 VARCHAR(1)  COMMENT 'CUST_TELEPHONE_NBR.DISC_MAJOR_CD IS MCDMJC'
   ,TN_DISC_DT                    INT         COMMENT 'CUST_TELEPHONE_NBR.TN_DISC_DT IS MCACMD'
   ,CUSTOMER_TN_TYPE_ID           VARCHAR(6)  COMMENT 'CUST_TELEPHONE_NBR.CUSTOMER_TN_TYPE_ID IS MCBHSM'
   ,TN_SRVC_CLASS                 VARCHAR(2)  COMMENT 'CUST_TELEPHONE_NBR.TN_SRVC_CLASS IS MCBHS0'
   ,CALL_CENTER_GROUPING_CD       VARCHAR(2)  COMMENT 'CUST_TELEPHONE_NBR.CALL_CENTER_GROUPING_CD IS MCAAD3'
   ,RING_CADENCE                  VARCHAR(2)  COMMENT 'CUST_TELEPHONE_NBR.RING_CADENCE IS MCAAPL'
   ,TN_BILL_WITH_ACCT             INT         COMMENT 'CUST_TELEPHONE_NBR.TN_BILL_WITH_ACCT IS MCBLT5'
   ,TN_BILL_WITH_STMT             INT         COMMENT 'CUST_TELEPHONE_NBR.TN_BILL_WITH_STMT IS MCBLT6'
   ,TN_EMS_ADDR_OVERRIDE          INT         COMMENT 'CUST_TELEPHONE_NBR.TN_EMS_ADDR_OVERRIDE IS MCBLUF'
   ,DISC_MINOR_REASON             VARCHAR(2)  COMMENT 'CUST_TELEPHONE_NBR.DISC_MINOR_REASON IS MCCDXS'
   ,CUST_TN_TYPE_ID_A             VARCHAR(6)  COMMENT 'CUST_TELEPHONE_NBR.CUST_TN_TYPE_ID_A IS MCBIBA'
   ,CREATED_USER_ID               VARCHAR(10) COMMENT 'CUST_TELEPHONE_NBR.CREATED_USER_ID IS MCA1US'
   ,CREATED_DT                    INT         COMMENT 'CUST_TELEPHONE_NBR.CREATED_DT IS MCA1DT'
   ,LAST_CHG_USER_ID              VARCHAR(10) COMMENT 'CUST_TELEPHONE_NBR.LAST_CHG_USER_ID IS MCA2US'
   ,LAST_CHG_DT                   INT         COMMENT 'CUST_TELEPHONE_NBR.LAST_CHG_DT IS MCA2DT'
   ,CUST_TELEPHONE_NBR_KEY        INT         COMMENT ''
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################