--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Deriving Smith table from incoming layer 
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

INSERT INTO TABLE ${hivevar:hive_database_name_smith}.${hivevar:smith_table} 
   PARTITION (SUITE_ID='${hivevar:suite_id}')
SELECT 
   PLUGIN_ID,
   PLUGIN_NAME,
   '${hivevar:dtm_created}',
   SOURCED_FROM_SYSTEM
FROM 
   ${hivevar:hive_database_name_incoming}.${hivevar:incoming_table} e 
WHERE NOT EXISTS
   (
    SELECT
       1 
    FROM 
       ${hivevar:hive_database_name_smith}.${hivevar:smith_table} d 
    WHERE 
       e.PLUGIN_ID = d.PLUGIN_ID AND d.SUITE_ID='${hivevar:suite_id}'
    )
     AND SUITE_ID='${hivevar:suite_id}';	
           
--##############################################################################
--#                                    End                                     #
--##############################################################################