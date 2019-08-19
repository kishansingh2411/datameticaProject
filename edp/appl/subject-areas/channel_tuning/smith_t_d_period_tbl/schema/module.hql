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
   DAY_TAG                     VARCHAR(8)      COMMENT '',
   PERIOD_ID                   INT        COMMENT '',
   PERIOD_DATE                 DATE      COMMENT '',
   YEAR                        SMALLINT      COMMENT '',
   QUARTER_NBR                 INT        COMMENT '',
   MONTH_NBR                   INT        COMMENT '',
   DAY_OF_THE_YEAR             SMALLINT      COMMENT '',
   DAY_OF_MONTH                INT        COMMENT '',
   QUARTER_NBR_OVERALL         SMALLINT      COMMENT '',
   MONTH_NBR_OVERALL           SMALLINT      COMMENT '',
   WEEK_NBR_OVERALL            INT        COMMENT '',
   DAY_NBR_OVERALL             INT        COMMENT '',
   WEEK_NBR_IN_YEAR            INT        COMMENT '',
   DAY_OF_WEEK                 VARCHAR(9)      COMMENT '',
   DAY_OF_WEEK_ABBR            CHAR(3)       COMMENT '',
   FISCAL_YEAR                 SMALLINT      COMMENT '',
   FISCAL_QUARTER              INT        COMMENT '',
   FISCAL_MONTH_NBR            INT        COMMENT '',
   FISCAL_MONTH_NBR_OVERALL    SMALLINT      COMMENT '',
   MONTH                       VARCHAR(9)      COMMENT '',
   MONTH_ABBR                  CHAR(3)       COMMENT '',
   FISCAL_QTR_OVERALL_TAG      CHAR(6)       COMMENT '',
   QUARTER_OVERALL_TAG         CHAR(6)       COMMENT '',
   MONTH_OVERALL_TAG           VARCHAR(7)      COMMENT '',
   LAST_DAY_IN_MONTH_IND       CHAR(1)      COMMENT '',
   LAST_DAY_OF_QUARTER_IND     CHAR(1)       COMMENT '',
   US_HOLIDAY_IND              CHAR(1)       COMMENT '',
   CANADIAN_HOLIDAY_IND        CHAR(1)       COMMENT '',
   PREV_MONTH_START_PERIOD_ID  INT        COMMENT '',
   PREV_MTD_PERIOD_ID          INT        COMMENT '',
   PREV_QTR_START_PERIOD_ID    INT        COMMENT '',
   PREV_QTD_PERIOD_ID          INT        COMMENT '',
   PREV_YEAR_START_PERIOD_ID   INT        COMMENT '',
   PREV_YTD_PERIOD_ID          INT        COMMENT '',
   PREV_MONTH_PERIOD_ID        INT        COMMENT '',
   PREV_QUARTER_PERIOD_ID      INT        COMMENT '',
   PREV_YEAR_PERIOD_ID         INT        COMMENT '',
   MONTH_TAG                   CHAR(6)       COMMENT '',
   DTM_CREATED                 TIMESTAMP      COMMENT '',
   DTM_LAST_UPDATED            TIMESTAMP      COMMENT '',
   WEEK_NBR_OVERALL_MON_TO_SUN INT        COMMENT '',
   WEEK_NBR_OVERALL_TUE_TO_MON INT        COMMENT '',
   WEEK_NBR_OVERALL_WED_TO_TUE INT        COMMENT '',
   WEEK_NBR_OVERALL_THU_TO_WED INT        COMMENT '',
   WEEK_NBR_OVERALL_FRI_TO_THU INT        COMMENT '',
   WEEK_NBR_OVERALL_SAT_TO_FRI INT        COMMENT '',
   BROADCAST_YEAR              SMALLINT      COMMENT '',
   BROADCAST_MONTH_NBR_OF_YEAR INT        COMMENT '',
   BROADCAST_WEEK_NBR_OF_YEAR  INT        COMMENT '',
   BROADCAST_DAY_NBR_OF_YEAR   SMALLINT      COMMENT '',
   BROADCAST_DAY_NBR_OF_WEEK   INT        COMMENT '',
   BROADCAST_DAY_NBR_OF_MONTH  INT        COMMENT '',
   BROADCAST_WEEK_DESC         VARCHAR(30)      COMMENT '',
   CALENDAR_WEEK_DESC          VARCHAR(30)      COMMENT '',
   BROADCAST_MONTH_DESC        VARCHAR(15)      COMMENT '',
   TGT_DTM_CREATED             TIMESTAMP      COMMENT '',
   TGT_DTM_LAST_UPDATED        TIMESTAMP      COMMENT '',
   BROADCAST_WEEK_ID           INT        COMMENT '',
   BROADCAST_MONTH_ID          INT        COMMENT '',
   CALENDAR_WEEK_ID            INT        COMMENT '',
   CALENDAR_MONTH_ID           INT        COMMENT '',
   DAY_OF_WEEK_ID              INT        COMMENT ''
)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################