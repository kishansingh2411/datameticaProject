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

set hive.execution.engine=tez
;

FROM 
(
   SELECT 
      TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('${hivevar:period_end_date}', 'yyyy-MM-dd'))) AS VISIT_DATE,
      'SITE_UNIQUE_VISITOR' AS COUNT_TYPE,
      null AS COUNT_TYPE_ID,
      'null' AS COUNT_TYPE_VALUE,
      COUNT(DISTINCT((CONCAT (d.POST_VISID_HIGH, '*', d.POST_VISID_LOW)))) as VISID_COUNT,
      -1 AS OPTIMUM_ID_COUNT,
      -1 AS CORP_HOUSE_CUST_COUNT, 
      '${hivevar:period_start_date}' AS PERIOD_START_DATE,
      '${hivevar:period_end_date}' AS PERIOD_END_DATE, 
      FROM_UNIXTIME(UNIX_TIMESTAMP()) AS DTM_CREATED, 
      'Omniture',
      'null' AS SITE_TYPE
   FROM
   (
    SELECT
	   PAGE_NAME_ID AS COUNT_TYPE_ID,
	   CASE WHEN EVAR1_PAGENAME IS NOT NULL THEN 
          EVAR1_PAGENAME 
	   ELSE 
	      PAGENAME 
	   END AS COUNT_TYPE_VALUE,
	   'PAGENAME' AS COUNT_TYPE, 
	   POST_VISID_HIGH, 
	   POST_VISID_LOW,  
	   '-1', 
       '-1',
       UPPER(SITE_TYPE) AS SITE_TYPE
    FROM ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
	WHERE
	   SUITE_NAME='${hivevar:suite_name}' 
	   AND SOURCE_DATE BETWEEN  '${hivevar:period_start_date}' 
	      AND '${hivevar:period_end_date}'
	   AND HIT_SOURCE  != 5 
	   AND HIT_SOURCE  != 7
	   AND HIT_SOURCE  != 8 
	   AND HIT_SOURCE  != 9 
	   AND EXCLUDE_HIT <= 0
	UNION ALL
	SELECT
	   SITE_SECTION_ID AS COUNT_TYPE_ID, 
	   SITE_SECTION_NAME AS COUNT_TYPE_VALUE,
	   'SITESECTION' AS COUNT_TYPE, 
	   POST_VISID_HIGH, POST_VISID_LOW,  
	   '-1', 
	   '-1',
	   UPPER(SITE_TYPE) AS SITE_TYPE
	FROM ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
	WHERE 
	   SUITE_NAME='${hivevar:suite_name}' 
	   AND SOURCE_DATE BETWEEN  '${hivevar:period_start_date}' 
	      AND '${hivevar:period_end_date}'
	   AND HIT_SOURCE  != 5 
	   AND HIT_SOURCE  != 7
	   AND HIT_SOURCE  != 8 
	   AND HIT_SOURCE  != 9 
	   AND EXCLUDE_HIT <= 0
    ) d
UNION ALL
SELECT 
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('${hivevar:period_end_date}', 'yyyy-MM-dd'))) AS VISIT_DATE,
   CONCAT('SITE_UNIQUE_VISITOR','_',
      CASE WHEN TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('${hivevar:period_end_date}', 'yyyy-MM-dd'))) < '${hivevar:cutoff_date}' OR  e.SITE_TYPE IS NULL THEN 
         'RESI'
      ELSE 
         e.SITE_TYPE
      END) AS COUNT_TYPE,
   null,
   'null' AS COUNT_TYPE_VALUE,
   COUNT(DISTINCT((CONCAT (e.POST_VISID_HIGH, '*', e.POST_VISID_LOW)))) AS VISID_COUNT,
   -1 AS OPTIMUM_ID_COUNT,
   -1 AS CORP_HOUSE_CUST_COUNT, 
   '${hivevar:period_start_date}' AS PERIOD_START_DATE,
   '${hivevar:period_end_date}' AS PERIOD_END_DATE, 
   FROM_UNIXTIME(UNIX_TIMESTAMP()) AS DTM_CREATED, 
   'Omniture', 
   CASE WHEN TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('${hivevar:period_end_date}', 'yyyy-MM-dd'))) < '${hivevar:cutoff_date}' OR  e.SITE_TYPE IS NULL THEN 
      'RESI' 
   ELSE 
      e.SITE_TYPE
   END AS SITE_TYPE
FROM
   (
   SELECT
      PAGE_NAME_ID AS COUNT_TYPE_ID,
      CASE WHEN EVAR1_PAGENAME IS NOT NULL THEN 
         EVAR1_PAGENAME 
      ELSE 
         PAGENAME 
      END AS COUNT_TYPE_VALUE,
      'PAGENAME' AS COUNT_TYPE, 
      POST_VISID_HIGH, 
      POST_VISID_LOW,  
      '-1', 
      '-1',
      SITE_TYPE AS SITE_TYPE
   FROM ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
   WHERE
      SUITE_NAME='${hivevar:suite_name}' 
      AND SOURCE_DATE BETWEEN  '${hivevar:period_start_date}' 
         AND '${hivevar:period_end_date}'
	  AND HIT_SOURCE  != 5 
	  AND HIT_SOURCE  != 7
	  AND HIT_SOURCE  != 8 
	  AND HIT_SOURCE  != 9 
	  AND EXCLUDE_HIT <= 0
   UNION ALL
   SELECT
      SITE_SECTION_ID AS COUNT_TYPE_ID, 
      SITE_SECTION_NAME AS COUNT_TYPE_VALUE,
      'SITESECTION' AS COUNT_TYPE, 
      POST_VISID_HIGH, POST_VISID_LOW,  
      '-1', 
      '-1',
      SITE_TYPE AS SITE_TYPE
   FROM ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
   WHERE 
      SUITE_NAME='${hivevar:suite_name}' 
      AND SOURCE_DATE BETWEEN  '${hivevar:period_start_date}' 
         AND '${hivevar:period_end_date}'
      AND HIT_SOURCE  != 5 
	  AND HIT_SOURCE  != 7
	  AND HIT_SOURCE  != 8 
	  AND HIT_SOURCE  != 9 
	  AND EXCLUDE_HIT <= 0
   ) e
   GROUP BY
      CASE WHEN TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('${hivevar:period_end_date}', 'yyyy-MM-dd'))) < '${hivevar:cutoff_date}' OR  e.SITE_TYPE IS NULL THEN 
         'RESI'
      ELSE 
         e.SITE_TYPE
      END
   UNION ALL
   SELECT
      TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP('${hivevar:period_end_date}', 'yyyy-MM-dd'))) AS VISIT_DATE, 
      f.COUNT_TYPE, 
      f.COUNT_TYPE_ID, 
      f.COUNT_TYPE_VALUE,
      COUNT(DISTINCT((CONCAT (f.POST_VISID_HIGH, '*', f.POST_VISID_LOW)))) AS VISID_COUNT,
      -1 AS OPTIMUM_ID_COUNT,
      -1 AS CORP_HOUSE_CUST_COUNT, 
      '${hivevar:period_start_date}' AS PERIOD_START_DATE,
      '${hivevar:period_end_date}' AS PERIOD_END_DATE, 
      FROM_UNIXTIME(UNIX_TIMESTAMP()) AS DTM_CREATED, 
      'Omniture',
      f.SITE_TYPE
   FROM
   (
    SELECT
       PAGE_NAME_ID AS COUNT_TYPE_ID,
       CASE WHEN EVAR1_PAGENAME IS NOT NULL THEN 
          EVAR1_PAGENAME 
       ELSE 
          PAGENAME 
       END AS COUNT_TYPE_VALUE,
       'PAGENAME' AS COUNT_TYPE, 
       POST_VISID_HIGH, 
       POST_VISID_LOW,  
       '-1', 
       '-1',
       SITE_TYPE AS SITE_TYPE
    FROM ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
    WHERE
       SUITE_NAME='${hivevar:suite_name}' 
       AND SOURCE_DATE BETWEEN  '${hivevar:period_start_date}' 
          AND '${hivevar:period_end_date}'
	   AND HIT_SOURCE  != 5 
	   AND HIT_SOURCE  != 7
	   AND HIT_SOURCE  != 8 
	   AND HIT_SOURCE  != 9 
	   AND EXCLUDE_HIT <= 0
    UNION ALL
    SELECT
       SITE_SECTION_ID AS COUNT_TYPE_ID, 
       SITE_SECTION_NAME AS COUNT_TYPE_VALUE,
       'SITESECTION' AS COUNT_TYPE, 
       POST_VISID_HIGH, POST_VISID_LOW,  
       '-1', 
       '-1',
       SITE_TYPE AS SITE_TYPE
    FROM ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
    WHERE 
       SUITE_NAME='${hivevar:suite_name}' 
       AND SOURCE_DATE BETWEEN  '${hivevar:period_start_date}' 
          AND '${hivevar:period_end_date}'
       AND HIT_SOURCE  != 5 
	   AND HIT_SOURCE  != 7
	   AND HIT_SOURCE  != 8 
	   AND HIT_SOURCE  != 9 
	   AND EXCLUDE_HIT <= 0
   ) f
   GROUP BY
      f.COUNT_TYPE_ID, 
      f.COUNT_TYPE_VALUE, 
      f.COUNT_TYPE,
      f.SITE_TYPE  
   ORDER BY 
      VISID_COUNT DESC
) h
INSERT OVERWRITE TABLE 
   ${hivevar:hive_database_name_gold}.${hivevar:gold_aggregate_count_tbl}
   PARTITION (SUITE_NAME='${hivevar:suite_name}', 
              FREQUENCY='${hivevar:frequency}',
              PERIOD='${hivevar:period}')			  
SELECT
   h.VISIT_DATE,
   h.COUNT_TYPE,
   h.COUNT_TYPE_ID,
   COUNT_TYPE_VALUE AS COUNT_TYPE_VALUE,
   h.VISID_COUNT,
   -1 AS OPTIMUM_ID_COUNT,
   -1 AS CORP_HOUSE_CUST_COUNT,
   h.PERIOD_START_DATE,
   h.PERIOD_END_DATE,
   h.dtm_created,
   'omniture',
   h.SITE_TYPE
   
INSERT OVERWRITE TABLE 
   ${hivevar:hive_database_name_smith}.${hivevar:smith_aggregate_count_tbl}
   PARTITION (SUITE_NAME='${hivevar:suite_name}', 
              FREQUENCY='${hivevar:frequency}',
              PERIOD='${hivevar:period}')
SELECT
   h.VISIT_DATE,
   h.COUNT_TYPE,
   h.COUNT_TYPE_ID,
   COUNT_TYPE_VALUE AS COUNT_TYPE_VALUE,
   h.VISID_COUNT,
   -1 AS OPTIMUM_ID_COUNT,
   -1 AS CORP_HOUSE_CUST_COUNT,
   h.PERIOD_START_DATE,
   h.PERIOD_END_DATE,
   h.DTM_CREATED,
   'omniture',
   h.SITE_TYPE
;

--##############################################################################
--#                                    End                                     #
--##############################################################################