--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Preaparing aggregate_unique_visitor_count for export from smith table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
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

set hive.execution.engine=tez;

INSERT OVERWRITE DIRECTORY '${hivevar:dir_path}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '~'  
   SELECT 
      CONCAT(VISIT_DATE,' 00:00:00'),
      COUNT_TYPE,COUNT_TYPE_ID,
      COUNT_TYPE_VALUE,
      VISID_COUNT,
      OPTIMUM_ID_COUNT, 
      CORP_HOUSE_CUST_COUNT,
      CONCAT(PERIOD_START_DATE,' 00:00:00'),
      CONCAT(PERIOD_END_DATE,' 00:00:00'),
      CURRENT_TIMESTAMP() as DTM_CREATED,
      SOURCED_FROM_SYSTEM,
      SUITE_NAME,
      FREQUENCY,
      PERIOD,
      SITE_TYPE 
   FROM ${hivevar:hive_database_name_smith}.${hivevar:smith_aggregate_unique_visitor_count_tbl}
   WHERE 
      SUITE_NAME='${hivevar:suite_name}' AND
      FREQUENCY='${hivevar:frequency}' AND 
      PERIOD='${hivevar:period}'
;	

--##############################################################################
--#                                    End                                     #
--##############################################################################