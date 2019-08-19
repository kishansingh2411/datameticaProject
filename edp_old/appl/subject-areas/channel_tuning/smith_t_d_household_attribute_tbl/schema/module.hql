--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(gold_nz_t_f_split_channel_tuning_rst_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : ...log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/30/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   HOUSEHOLD_ATTR_ID        INT       COMMENT '',
   HOUSEHOLD_ID             INT      COMMENT '',
   DTM_CREATED              TIMESTAMP    COMMENT '',
   DTM_LAST_UPDATED         TIMESTAMP    COMMENT '',
   CREATED_BY               VARCHAR(50)     COMMENT '',
   LAST_UPDATED_BY          VARCHAR(50)     COMMENT '',
   MFLAG                    VARCHAR(1)      COMMENT '',
   CORP                     INT      COMMENT '',
   COHORT                   VARCHAR(3)    COMMENT '',
   COHORT_TEXT              VARCHAR(75)    COMMENT '',
   ETHNIC                   VARCHAR(2)    COMMENT '',
   LANGUAGE                 VARCHAR(2)    COMMENT '',
   GROUPCODE                VARCHAR(2)    COMMENT '',
   COUNTRY                  VARCHAR(2)    COMMENT '',
   CUST_RES                 VARCHAR(2)    COMMENT '',
   ENHANCED_EST_INCOME      VARCHAR(1)    COMMENT '',
   I1_COMBINED_AGE          VARCHAR(3)    COMMENT '',
   I1_GENDERCODE            VARCHAR(1)    COMMENT '',
   HOUSEHOLD_COMPOSITION    VARCHAR(1)    COMMENT '',
   DWELLING_TYPE            VARCHAR(1)    COMMENT '',
   LENGTH_OF_RESIDENCE      VARCHAR(2)    COMMENT '',
   NUMBER_OF_CHILDREN       VARCHAR(1)    COMMENT '',
   I2_EXACT_AGE             VARCHAR(2)    COMMENT '',
   I1_OCCUPATION            VARCHAR(2)    COMMENT '',
   OCCUPATION_GROUP1        VARCHAR(3)    COMMENT '',
   EDUCATION_INDIVIDUAL1    VARCHAR(2)    COMMENT '',
   PRODUCT_ID               INT       COMMENT '',
   MDM_TIER_RPT             INT       COMMENT '',
   ZIPCODE                  VARCHAR(10)    COMMENT '',
   PRODUCT_CODE             VARCHAR(10)    COMMENT '',
   ETHNIC1                  VARCHAR(27)    COMMENT '',
   LNGUAGE                  VARCHAR(58)    COMMENT '',
   COUNTRY1                 VARCHAR(27)    COMMENT '',
   GROUP1                   VARCHAR(43)    COMMENT '',
   DWELLT                   VARCHAR(21)    COMMENT '',
   LNTH_RES                 VARCHAR(11)    COMMENT '',
   INCOME                   VARCHAR(20)    COMMENT '',
   GEND                     VARCHAR(7)    COMMENT '',
   AGE1                     VARCHAR(11)    COMMENT '',
   ACCULTUR                 VARCHAR(11)    COMMENT '',
   ACCULTURIZATION1         VARCHAR(31)    COMMENT '',
   HOUSEHOLD_KEY2           VARCHAR(32)    COMMENT '',
   RECORDIND                VARCHAR(2)    COMMENT '',
   TGT_DTM_CREATED          TIMESTAMP    COMMENT '',
   TGT_DTM_LAST_UPDATED     TIMESTAMP    COMMENT ''
)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################