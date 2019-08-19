--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Deriving Smith Derived tables from gold layer 
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

--# Deriving optimum isp table at smith layer

INSERT INTO TABLE  ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}SMITH_OPTIMUM_ISP
   PARTITION (SUITE_ID='${hivevar:suite_id}')
SELECT 
   DISTINCT 
      ISP_ID, 
      ISP_NAME,
      '${hivevar:source_date}', 
      '2999-12-31', 
      SOURCED_FROM_SYSTEM, 
      CURRENT_TIMESTAMP
FROM 
   ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl} f
WHERE NOT EXISTS
   (
    SELECT 
       1 
    FROM 
       ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}SMITH_OPTIMUM_ISP d
    WHERE
       f.ISP_ID = d.ISP_ID and SUITE_ID='${hivevar:suite_id}'
    ) 
     AND f.SOURCE_DATE='${hivevar:source_date}' 
     AND f.ISP_NAME IS NOT NULL
     AND f.ISP_ID IS NOT NULL
;

--# Deriving optimum pagename table at smith layer

INSERT INTO TABLE  ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}SMITH_OPTIMUM_PAGENAMES
   PARTITION (SUITE_ID='${hivevar:suite_id}')
SELECT 
   DISTINCT 
      PAGE_NAME_ID, 
      EVAR1_PAGENAME AS PAGENAME,
      '${hivevar:source_date}', 
      '2999-12-31', 
      SOURCED_FROM_SYSTEM, 
      CURRENT_TIMESTAMP
FROM ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl} f
WHERE NOT EXISTS
   (
    SELECT 
       1 
    FROM 
       ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}SMITH_OPTIMUM_PAGENAMES d
    WHERE
       f.PAGE_NAME_ID = d.PAGE_NAME_ID and SUITE_ID='${hivevar:suite_id}'
   )
    AND f.SOURCE_DATE='${hivevar:source_date}' 
    AND f.EVAR1_PAGENAME IS NOT NULL
    AND f.PAGE_NAME_ID IS NOT NULL
;

--# Deriving optimum site section table at smith layer

INSERT INTO TABLE  ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}SMITH_OPTIMUM_SITE_SECTIONS
   PARTITION (SUITE_ID='${hivevar:suite_id}')
SELECT 
   DISTINCT 
      SITE_SECTION_ID, 
      SITE_SECTION_NAME,
      '${hivevar:source_date}', 
      '2999-12-31', 
      SOURCED_FROM_SYSTEM, 
      CURRENT_TIMESTAMP
FROM 
    ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl} f
WHERE NOT EXISTS
   (
    SELECT 
       1 
    FROM 
       ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}smith_optimum_site_sections d
    WHERE
       f.SITE_SECTION_ID = d.SITE_SECTION_ID and SUITE_ID='${hivevar:suite_id}'
    ) 
     AND f.SOURCE_DATE='${hivevar:source_date}'
     AND f.SITE_SECTION_NAME IS NOT NULL
     AND f.SITE_SECTION_ID IS NOT NULL
;

--# Deriving optimum error message table at Smith layer


 INSERT INTO TABLE  ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}smith_optimum_err_message
   PARTITION (SUITE_ID='${hivevar:suite_id}')
SELECT 
   DISTINCT 
      OPT_ERR_MSG_ID, 
      PROP35_OPT_ERR_MSG_DESC,
      '${hivevar:source_date}', 
      '2999-12-31',
      SOURCED_FROM_SYSTEM, 
      CURRENT_TIMESTAMP
FROM 
    ${hivevar:hive_database_name_gold}.${hivevar:gold_hit_data_tbl} f
WHERE NOT EXISTS
   (
    SELECT 
       1 
    FROM 
       ${hivevar:hive_database_name_smith}.${hivevar:table_prefix}smith_optimum_err_message d
    WHERE
       f.OPT_ERR_MSG_ID = d.OPT_ERR_MSG_ID and SUITE_ID='${hivevar:suite_id}'
    ) 
     AND f.SOURCE_DATE='${hivevar:source_date}'
     AND f.PROP35_OPT_ERR_MSG_DESC IS NOT NULL
     AND f.OPT_ERR_MSG_ID IS NOT NULL
;

--##############################################################################
--#                                    End                                     #
--##############################################################################