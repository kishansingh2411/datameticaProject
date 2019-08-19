--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from GOLD TABLE
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 12/08/2016
--#   Log File    : .../log/ovcdr/OVCDR_*.log
--#   SQL File    : 
--#   Error File  : .../log/ovcdr/OVCDR_*.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/08/2016       Initial version
--#
--#
--#####################################################################################################################

INSERT INTO TABLE
   ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_metadata_table} 
SELECT 
   DISTINCT BATCH_NBR, 
   USAGE_DATE    
FROM 
   ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################