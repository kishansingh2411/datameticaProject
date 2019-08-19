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
--#   Date        : 12/28/2015
--#   Log File    : .../log/bering_media/BERING_MEDIA_MOVE_TO_GOLD_JOB.log
--#   SQL File    : 
--#   Error File  : .../log/bering_media/BERING_MEDIA_MOVE_TO_GOLD_JOB.log
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

set hive.exec.dynamic.partition.mode=nonstrict
;

INSERT OVERWRITE TABLE
   ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table} 
   PARTITION (USAGE_DATE)
SELECT 
   TELEPHONE_NUMBER  ,  
   CALL_TYPE    ,    
   SECONDS_OF_USE ,  
   NUMBER_OF_CALLS  ,   
   BATCH_NBR     ,   
   ATTEMPT_COUNT  ,  
   CORP        ,     
   HOUSE       ,     
   CUST        ,     
   DTM_CREATED  ,    
   DTM_LAST_MODIFIED,
   FILE_NAME,
   USAGE_DATE    
FROM 
   ${hivevar:work_database}.${hivevar:table_prefix}${hivevar:work_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################