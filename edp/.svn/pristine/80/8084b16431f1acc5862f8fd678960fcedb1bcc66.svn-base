--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_customer_telephone)
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
   SITE_ID                       INT                COMMENT 'CUSTOMER_TELEPHONE.SITE_ID IS GTNROV'
   ,ACCT_NBR                     INT                COMMENT 'CUSTOMER_TELEPHONE.ACCT_NBR IS GTCNBR'
   ,CUSTOMER_KEY                 INT                COMMENT ''
   ,SRVC_CATG_CD                 VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.SRVC_CATG_CD IS GTCEK6'
   ,SRVC_OCCUR                   INT                COMMENT 'CUSTOMER_TELEPHONE.SRVC_OCCUR IS GTSYP8'
   ,NO_SUFFIX                    TINYINT            COMMENT 'CUSTOMER_TELEPHONE.NO_SUFFIX IS GTTRNB'
   ,CUST_PHONE_STS               VARCHAR(2)         COMMENT 'CUSTOMER_TELEPHONE.CUST_PHONE_STS IS GTSUFJ'
   ,CUST_TEL_STS_DT              INT                COMMENT 'CUSTOMER_TELEPHONE.CUST_TEL_STS_DT IS GTDDFT'
   ,LAST_WORK_ORDER_NBR          INT                COMMENT 'CUSTOMER_TELEPHONE.LAST_WORK_ORDER_NBR IS GTSYMQ'
   ,LAST_WORK_ORDER_KEY          INT                COMMENT ''
   ,LAST_WORK_ORDER_DT           INT                COMMENT 'CUSTOMER_TELEPHONE.LAST_WORK_ORDER_DT IS GTDDFU'
   ,NON_PUBLISHED_CD             VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.NON_PUBLISHED_CD IS GTSUFR'
   ,LOCAL_SRVC_CHARGE            DECIMAL(7,2)       COMMENT 'CUSTOMER_TELEPHONE.LOCAL_SRVC_CHARGE IS GTVAUG'
   ,EXCEPTION_CD_1               VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.EXCEPTION_CD_1 IS GTCVFX'
   ,EXCEPTION_CD_2               VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.EXCEPTION_CD_2 IS GTCVFY'
   ,CUST_TEL_INSTALL_DT          INT                COMMENT 'CUSTOMER_TELEPHONE.CUST_TEL_INSTALL_DT IS GTDDFV'
   ,CUST_TEL_DISC_DT             INT                COMMENT 'CUSTOMER_TELEPHONE.CUST_TEL_DISC_DT IS GTDDFW'
   ,TOLL_LIMIT                   DECIMAL(9,2)       COMMENT 'CUSTOMER_TELEPHONE.TOLL_LIMIT IS GTPRAG'
   ,TOLL_USAGE                   DECIMAL(9,2)       COMMENT 'CUSTOMER_TELEPHONE.TOLL_USAGE IS GTPRAH'
   ,DIR_BT_ENTRY                 VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.DIR_BT_ENTRY IS GTSUFN'
   ,DIR_PHONE_NBR                BIGINT             COMMENT 'CUSTOMER_TELEPHONE.DIR_PHONE_NBR IS GTWSNB'
   ,DIR_ENTRY_STYLE              VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.DIR_ENTRY_STYLE IS GTSUFO'
   ,DIR_ENTRY_TYPE               VARCHAR(3)         COMMENT 'CUSTOMER_TELEPHONE.DIR_ENTRY_TYPE IS GTSUFP'
   ,DIR_NM                       VARCHAR(30)        COMMENT 'CUSTOMER_TELEPHONE.DIR_NM IS GTTTSL'
   ,DIR_TYPEFACE                 VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.DIR_TYPEFACE IS GTSUFQ'
   ,DIR_BUSINESS_DESC            VARCHAR(32)        COMMENT 'CUSTOMER_TELEPHONE.DIR_BUSINESS_DESC IS GTTTSK'
   ,TOT_PLAN_CHARGES             DECIMAL(9,2)       COMMENT 'CUSTOMER_TELEPHONE.TOT_PLAN_CHARGES IS GTVAUE'
   ,NO_OF_LINES                  INT                COMMENT 'CUSTOMER_TELEPHONE.NO_OF_LINES IS GTQTPF'
   ,PRIORITY                     VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.PRIORITY IS GTPRIY'
   ,PRIM_ADL                     VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.PRIM_ADL IS GTSUFV'
   ,TEL_CONTRACT_NBR             VARCHAR(11)        COMMENT 'CUSTOMER_TELEPHONE.TEL_CONTRACT_NBR IS GTNAB8'
   ,DEPOSIT_AMT                  DECIMAL(9,2)       COMMENT 'CUSTOMER_TELEPHONE.DEPOSIT_AMT IS GTDEPS'
   ,TOLL_DETAIL_THRESHOLD        DECIMAL(7,2)       COMMENT 'CUSTOMER_TELEPHONE.TOLL_DETAIL_THRESHOLD IS GTSYMM'
   ,DISC_MAJOR_CD                VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.DISC_MAJOR_CD IS GTDMJC'
   ,CUST_TEL_INSTALL_TM          INT                COMMENT 'CUSTOMER_TELEPHONE.CUST_TEL_INSTALL_TM IS GTANAC'
   ,CUST_TEL_DISC_TM             INT                COMMENT 'CUSTOMER_TELEPHONE.CUST_TEL_DISC_TM IS GTANAD'
   ,TYPE_OF_SRVC                 VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.TYPE_OF_SRVC IS GTALDN'
   ,USI_BNS_CD                   VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.USI_BNS_CD IS GTSUIX'
   ,CNAM_PRESENTATION_IND        VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.CNAM_PRESENTATION_IND IS GTALIH'
   ,LINE_DIRECTION               VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.LINE_DIRECTION IS GTALRN'
   ,SOFT_DIAL_TONE_ACTIVE        VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.SOFT_DIAL_TONE_ACTIVE IS GTAVZL'
   ,SDT_BEGIN_DATE               INT                COMMENT 'CUSTOMER_TELEPHONE.SDT_BEGIN_DATE IS GTACKR'
   ,SDT_END_DT                   INT                COMMENT 'CUSTOMER_TELEPHONE.SDT_END_DT IS GTACKS'
   ,IPIC                         VARCHAR(5)         COMMENT 'CUSTOMER_TELEPHONE.IPIC IS GTBHV9'
   ,IPIC_FREEZE                  VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.IPIC_FREEZE IS GTAVZI'
   ,LPIC_FREEZE                  VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.LPIC_FREEZE IS GTAVZJ'
   ,PIC_FREEZE                   VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.PIC_FREEZE IS GTAVZK'
   ,NPA_STD_NO                   INT                COMMENT 'CUSTOMER_TELEPHONE.NPA_STD_NO IS GTUDNB'
   ,NXX_EXCHANGE_NO              INT                COMMENT 'CUSTOMER_TELEPHONE.NXX_EXCHANGE_NO IS GTUCNB'
   ,THOUSAND_NO                  INT                COMMENT 'CUSTOMER_TELEPHONE.THOUSAND_NO IS GTWQNB'
   ,NPA_STD_NO_PORTED_TN         INT                COMMENT 'CUSTOMER_TELEPHONE.NPA_STD_NO_PORTED_TN IS GTSYUN'
   ,NXX_EXCHANGE_NO_PORTED_TN    INT                COMMENT 'CUSTOMER_TELEPHONE.NXX_EXCHANGE_NO_PORTED_TN IS GTSYUO'
   ,THOUSAND_NO_PORTED_TN        INT                COMMENT 'CUSTOMER_TELEPHONE.THOUSAND_NO_PORTED_TN IS GTSYUP'
   ,TOLL_CARRIER_ID              VARCHAR(5)         COMMENT 'CUSTOMER_TELEPHONE.TOLL_CARRIER_ID IS GTCETA'
   ,SRVC_CATG                    VARCHAR(3)         COMMENT 'CUSTOMER_TELEPHONE.SRVC_CATG IS GTAAFD'
   ,LEC_FREEZE                   VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.LEC_FREEZE IS GTALHG'
   ,INTRA_LATA_PIC_TOLL_CAR      VARCHAR(5)         COMMENT 'CUSTOMER_TELEPHONE.INTRA_LATA_PIC_TOLL_CAR IS GTCVFZ'
   ,CO_ID                        VARCHAR(6)         COMMENT 'CUSTOMER_TELEPHONE.CO_ID IS GTUHCD'
   ,SWITCH_ID                    VARCHAR(15)        COMMENT 'CUSTOMER_TELEPHONE.SWITCH_ID IS GTU7CD'
   ,PRINT_ITEMIZED_USAGE         VARCHAR(1)         COMMENT 'CUSTOMER_TELEPHONE.PRINT_ITEMIZED_USAGE IS GTAWHU'
   ,DISCONNECT_MINOR_REASON      VARCHAR(2)         COMMENT 'CUSTOMER_TELEPHONE.DISCONNECT_MINOR_REASON IS GTCDXS'
   ,SRVC_CLASS                   VARCHAR(2)         COMMENT 'CUSTOMER_TELEPHONE.SRVC_CLASS IS GTU1CD'
   ,TELEPHONE_SRV_CTGY_CD        VARCHAR(3)         COMMENT 'CUSTOMER_TELEPHONE.TELEPHONE_SRV_CTGY_CD IS GTAALI'
   ,SIGNALLING_TYPE_CD           VARCHAR(2)         COMMENT 'CUSTOMER_TELEPHONE.SIGNALLING_TYPE_CD IS GTAALJ'
   ,DIALLING_TYPE_CD             VARCHAR(2)         COMMENT 'CUSTOMER_TELEPHONE.DIALLING_TYPE_CD IS GTAALK'
   ,CUSTOMER_TELEPHONE_KEY       INT                COMMENT ''
)
PARTITIONED BY (SOURCE_DATE STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################