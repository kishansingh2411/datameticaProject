--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Smith table from gold 
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/${suite_name}/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${suite_name}/${job_name}.log
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

set hive.execution.engine=tez;

INSERT OVERWRITE DIRECTORY '${hivevar:location}' 
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '\t'
 SELECT
              '' as ALERTS_NOTIFICATION     ,
             BROWSER_ID as BROWSER_ID  ,
              CAMPAIGN as CAMPAIGN     ,
             '' as CAMPAIGN_EVAR           ,
              CONNECTION_TYPE_ID as CONNECTION_TYPE_ID      ,
              CORP as CORP                    ,
              COUNTRY_ID as COUNTRY_ID          ,
              EVAR24_CSR_ID as CSR_ID          ,
              CUST as CUST                              ,
              PROP10 as CUSTOM_LINK                     ,
              EVAR8_CUSTOM_LINK_TRACKING as CUSTOM_LINK_TRACKING    ,
              DWELLING_NBR as DWELLING_NBR         ,
              EVAR14_EMAIL as EMAIL               ,
              EVENT_LIST as EVENT_LIST           ,
              PROP25 as HULU_ID                 ,
              ISP_ID as ISP_ID                  ,
              JAVASCRIPT_VERSION_ID as JAVASCRIPT_VERSION_ID   ,
              LANGUAGE_ID as LANGUAGE_ID             ,
              MOBILE_ID as MOBILE_ID                    ,
              MONTHLY_VISITOR as MONTHLY_VISITOR         ,
              OPERATING_SYSTEM_ID as OPERATING_SYSTEM_ID     ,
             '' as  OPT_ERR_MSG_ID              ,
              EVAR11_OPTIMUM_ID as OPTIMUM_ID         ,
              OPTIMUM_USER_ID as OPTIMUM_USER_ID     ,
              PAGE_EVENT as PAGE_EVENT              ,
              PAGE_EVENT_VAR1 as PAGE_EVENT_VAR1     ,
              PAGE_NAME_ID as PAGE_NAME_ID          ,
              PAGE_TYPE as PAGE_TYPE             ,
              PAGE_URL as PAGE_URL                ,
              PLUGINS as PLUGINS                 ,
              POST_VISID_HIGH as POST_VISID_HIGH         ,
              POST_VISID_LOW as POST_VISID_LOW          ,
              REFERRER_TYPE_ID as REFERRER_TYPE_ID        ,
              SEARCH_ENGINE_ID as SEARCH_ENGINE_ID        ,
              SITE_SECTION_ID as SITE_SECTION_ID        ,
              SOURCED_FROM_SYSTEM as SOURCED_FROM_SYSTEM     ,
              ${hivevar:suite_id} as SUITE_ID            ,
              EVAR26 as TACTIC                  ,
              EVAR16_TEL_NBR as TEL_NBR          ,
              EVAR23 as TTT_ACCT_NUMBER         ,
              VISID_HIGH as VISID_HIGH             ,
               VISID_LOW  as VISID_LOW                   ,
              VISIT_DATE as VISIT_DATE              ,
              VISIT_NUM as VISIT_NUM               ,
              VISIT_PAGE_NUM as VISIT_PAGE_NUM          ,
              VISIT_START_TIME_GMT as VISIT_START_TIME_GMT    ,
              VISID_TIMESTAMP as VISIT_TIMESTAMP ,
              EVAR43 as WILDCARD 
FROM  ${hivevar:hive_database_name_gold}.${hivevar:gold_vow_hit_data_tbl}
WHERE SOURCE_DATE='${hivevar:source_date}'
;
              