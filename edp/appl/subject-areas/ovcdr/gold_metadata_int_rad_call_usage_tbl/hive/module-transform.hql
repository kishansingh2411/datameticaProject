--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold Metadata table from GOLD TABLE
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/20/2017
--#   Log File    : .../log/ovcdr/EDP_*.log
--#   SQL File    : 
--#   Error File  : .../log/ovcdr/EDP_*.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          01/20/2017       Initial version
--#
--#
--#####################################################################################################################

DELETE FROM 
   ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_metadata_table}
WHERE 
   BATCH_NBR IN (
      SELECT 
         DISTINCT BATCH_ID
      FROM ${hivevar:incoming_database}.${hivevar:incoming_table}
      WHERE
         SOURCE_DATE = '${source_date}'
   )
;

INSERT INTO TABLE
   ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_metadata_table} 
SELECT 
   DISTINCT BATCH_ID, 
   USAGE_DATE    
FROM 
   ${hivevar:incoming_database}.${hivevar:incoming_table}
WHERE
   SOURCE_DATE = '${source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################