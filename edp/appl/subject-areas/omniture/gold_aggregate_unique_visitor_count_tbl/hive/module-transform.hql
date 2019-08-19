--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold aggeregate unique visitor count table from Gold hit data table
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

FROM
   (
    SELECT
       PAGE_NAME_ID AS COUNT_TYPE_ID,
       CASE WHEN EVAR1_PAGENAME IS NOT NULL 
       THEN 
          EVAR1_PAGENAME 
       ELSE 
          PAGENAME 
       END AS COUNT_TYPE_VALUE,
       'PAGENAME' AS COUNT_TYPE, 
       POST_VISID_HIGH, 
       POST_VISID_LOW,  
       ${hiveconf:column_name}, 
       CORP
    FROM
       ${hiveconf:hive_database_name_gold}.${hiveconf:gold_hit_data_tbl}
    WHERE
       SUITE_NAME='${hiveconf:suite_name}' AND 
       SOURCE_DATE BETWEEN  '${hiveconf:period_start_date}' AND 
       '${hiveconf:period_end_date}'
    UNION ALL
    SELECT
       SITE_SECTION_ID AS COUNT_TYPE_ID, 
       SITE_SECTION_NAME AS COUNT_TYPE_VALUE,
       'SITESECTION' AS COUNT_TYPE, 
       POST_VISID_HIGH, 
       POST_VISID_LOW,  
       ${hiveconf:column_name}, 
       CORP
    FROM
       ${hiveconf:hive_database_name_gold}.${hiveconf:gold_hit_data_tbl}
    WHERE 
       SUITE_NAME='${hiveconf:suite_name}' AND 
       SOURCE_DATE BETWEEN '${hiveconf:period_start_date}' AND 
       '${hiveconf:period_end_date}'
        ) e

INSERT OVERWRITE TABLE ${hiveconf:hive_database_name_gold}.${hiveconf:gold_aggregate_unique_visitor_count_tbl}
   PARTITION (SUITE_NAME='${hiveconf:suite_name}', 
              FREQUENCY='${hiveconf:frequency}',
              PERIOD='${hiveconf:period}')
SELECT
   TO_DATE(from_unixtime(UNIX_TIMESTAMP('${hiveconf:period_end_date}', 'yyyy-mm-dd'))), 
   e.COUNT_TYPE, 
   e.COUNT_TYPE_ID, 
   e.COUNT_TYPE_VALUE,
   COUNT(DISTINCT((CONCAT (e.POST_VISID_HIGH, '*', e.POST_VISID_LOW)))) AS VISID_COUNT,
   COUNT(DISTINCT e.${hiveconf:column_name}) AS OPTIMUM_ID_COUNT,
   COUNT(DISTINCT(e.CORP!='-1')) AS CORP_HOUSE_CUST_COUNT, 
   '${hiveconf:period_start_date}' AS PERIOD_START_DATE,
   '${hiveconf:period_end_date}' AS PERIOD_END_DATE, 
   from_unixtime(unix_timestamp()), 
   'Omniture'
GROUP BY
   e.COUNT_TYPE_ID, 
   e.COUNT_TYPE_VALUE, 
   e.COUNT_TYPE  
ORDER BY 
   VISID_COUNT DESC 

INSERT OVERWRITE TABLE ${hiveconf:hive_database_name_smith}.${hiveconf:smith_aggregate_unique_visitor_count_tbl}
   PARTITION (SUITE_NAME='${hiveconf:suite_name}', 
              FREQUENCY='${hiveconf:frequency}',
              PERIOD='${hiveconf:period}')
SELECT
   TO_DATE(from_unixtime(UNIX_TIMESTAMP('${hiveconf:period_end_date}', 'yyyy-mm-dd'))), 
   e.COUNT_TYPE, 
   e.COUNT_TYPE_ID, 
   e.COUNT_TYPE_VALUE,
   COUNT(DISTINCT((CONCAT (e.POST_VISID_HIGH, '*', e.POST_VISID_LOW)))) AS VISID_COUNT,
   COUNT(DISTINCT e.${hiveconf:column_name}) AS OPTIMUM_ID_COUNT,
   COUNT(DISTINCT(e.CORP!='-1')) AS CORP_HOUSE_CUST_COUNT, 
   '${hiveconf:period_start_date}' AS PERIOD_START_DATE,
   '${hiveconf:period_end_date}' AS PERIOD_END_DATE, 
   from_unixtime(unix_timestamp()), 
   'Omniture'
GROUP BY
   e.COUNT_TYPE_ID, 
   e.COUNT_TYPE_VALUE, 
   e.COUNT_TYPE  
ORDER BY 
   VISID_COUNT DESC 
;

INSERT INTO TABLE ${hiveconf:hive_database_name_gold}.${hiveconf:gold_aggregate_unique_visitor_count_tbl}
   PARTITION (SUITE_NAME='${hiveconf:suite_name}', 
              FREQUENCY='${hiveconf:frequency}',
              PERIOD='${hiveconf:period}')
 select TO_DATE(from_unixtime(UNIX_TIMESTAMP('${hiveconf:period_end_date}', 'yyyy-MM-dd'))),
 'SITE_UNIQUE_VISITOR' ,
  null,
  'null',
  COUNT(DISTINCT((CONCAT (POST_VISID_HIGH, '*', POST_VISID_LOW)))),
  -1 AS OPTIMUM_ID_COUNT,
   -1 AS CORP_HOUSE_CUST_COUNT, 
   '${hiveconf:period_start_date}' AS PERIOD_START_DATE,
   '${hiveconf:period_end_date}' AS PERIOD_END_DATE, 
   from_unixtime(UNIX_TIMESTAMP()), 
   'Omniture'
   FROM 
     ${hiveconf:hive_database_name_gold}.${hiveconf:gold_hit_data_tbl}
   WHERE 
       SUITE_NAME='${hiveconf:suite_name}' 
       AND SOURCE_DATE BETWEEN '${hiveconf:period_start_date}' 
                       AND '${hiveconf:period_end_date}'     
 ;
 

--##############################################################################
--#                                    End                                     #
--##############################################################################