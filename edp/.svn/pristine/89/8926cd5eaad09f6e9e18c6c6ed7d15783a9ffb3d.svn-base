--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold aggeregate visit count table from Gold hit data table
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
       DISTINCT 
          'PAGENAME' AS COUNT_TYPE,
          PAGE_NAME_ID AS COUNT_TYPE_ID,
          CASE WHEN EVAR1_PAGENAME IS NOT NULL 
          THEN 
             EVAR1_PAGENAME 
          ELSE 
             PAGENAME 
          END AS COUNT_TYPE_VALUE,
          POST_VISID_HIGH, 
          POST_VISID_LOW, 
          VISIT_NUM
    FROM
       ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
    WHERE
	       SUITE_NAME='${hivevar:suite_name}' 
	       AND SOURCE_DATE='${hivevar:period_start_date}'
	       AND hit_source  != 5 
	       AND hit_source  != 7
	       AND hit_source  != 8 
	       AND hit_source  != 9 
	       AND exclude_hit <= 0
    UNION ALL
    SELECT
       DISTINCT 
          'SITESECTION' AS COUNT_TYPE,
          SITE_SECTION_ID AS COUNT_TYPE_ID, 
          SITE_SECTION_NAME AS COUNT_TYPE_VALUE, 
          POST_VISID_HIGH, 
          POST_VISID_LOW, 
          VISIT_NUM
    FROM
       ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl}
    WHERE
       SUITE_NAME='${hivevar:suite_name}' 
       AND SOURCE_DATE='${hivevar:period_start_date}'
    )e 

INSERT OVERWRITE TABLE ${hivevar:hive_database_name_gold}.${hivevar:gold_aggregate_count_tbl}
   PARTITION (SUITE_NAME='${hivevar:suite_name}', 
              FREQUENCY='${hivevar:frequency}',
              PERIOD='${hivevar:period}')
SELECT 
   e.COUNT_TYPE,
   e.COUNT_TYPE_ID,
   e.COUNT_TYPE_VALUE,
   COUNT(*),
   CURRENT_TIMESTAMP, 
   'Omniture'
GROUP BY 
   e.COUNT_TYPE,
   e.COUNT_TYPE_ID,
   e.COUNT_TYPE_VALUE
   
INSERT OVERWRITE TABLE ${hivevar:hive_database_name_smith}.${hivevar:smith_aggregate_count_tbl}
   PARTITION (SUITE_NAME='${hivevar:suite_name}', 
              FREQUENCY='${hivevar:frequency}',
              PERIOD='${hivevar:period}')
SELECT 
   e.COUNT_TYPE,
   e.COUNT_TYPE_ID,
   e.COUNT_TYPE_VALUE,
   COUNT(*),
   CURRENT_TIMESTAMP, 
   'Omniture'
GROUP BY 
   e.COUNT_TYPE,
   e.COUNT_TYPE_ID,
   e.COUNT_TYPE_VALUE
;

--##############################################################################
--#                                    End                                     #
--##############################################################################