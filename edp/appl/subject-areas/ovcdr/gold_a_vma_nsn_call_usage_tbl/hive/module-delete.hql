--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Incoming table
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

set hive.exec.dynamic.partition.mode=nonstrict
;

INSERT OVERWRITE TABLE
   ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table} 
   PARTITION (USAGE_DATE)
SELECT 
   TELEPHONE_NUMBER, 
   ON_NET_IND  ,     
   NUMBER_OF_CALLS,  
   BATCH_NBR ,       
   ATTEMPT_COUNT ,   
   CORP    ,         
   HOUSE    ,       
   CUST     ,        
   DTM_CREATED ,     
   DTM_LAST_MODIFIED,
   FILE_NAME,
   BHV_INDICATOR,
   CUST_ACCT_NUMBER,
   SOURCE_SYSTEM_ID,
   USAGE_DATE    
FROM ${hivevar:work_database}.${hivevar:table_prefix}${hivevar:work_table}
WHERE 
   BATCH_NBR NOT IN(
      SELECT 
         DISTINCT BATCH_ID 
	  FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
      WHERE 
         SOURCE_DATE='${hivevar:source_date}'
    )
;

--##############################################################################
--#                                    End                                     #
--##############################################################################