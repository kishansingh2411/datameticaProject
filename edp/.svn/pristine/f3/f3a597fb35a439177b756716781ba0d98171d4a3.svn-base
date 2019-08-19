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
--#   Date        : 12/28/2016
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
--#    1.0     DataMetica Team          12/28/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.mapred.supports.subdirectories=true;

INSERT OVERWRITE TABLE 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} 
SELECT 
      unionall.OV_TELEPHONE_NUMBER_ID  ,    
      unionall.CORP   ,     
	  unionall.TELEPHONE_NUMBER ,      
      unionall.AREA_CODE      ,        
	  unionall.PHONE_EXCHANGE  ,       
	  unionall.EXCHANGE_EXTENTION  ,   
	  unionall.CUSTOMER_ACCOUNT_ID  ,  
	  unionall.ID_IPTEL_ORDER ,        
	  unionall.SOURCED_FROM_SYSTEM ,   
	  unionall.LAST_UPDATED_FROM_SYSTEM,
	  unionall.DTM_CREATED  ,          
	  unionall.DTM_EFFTV      ,        
	  unionall.DTM_EXPIRED     ,       
	  unionall.DTM_LAST_MODIFIED ,
	  unionall.DTM_LAST_UPDATED 
            	  
FROM 
 (SELECT  
      incoming.OV_TELEPHONE_NUMBER_ID ,
      incoming.CORP	,   
	  incoming.TELEPHONE_NUMBER ,      
	  incoming.AREA_CODE      ,        
	  incoming.PHONE_EXCHANGE  ,       
	  incoming.EXCHANGE_EXTENTION  ,   
	  incoming.CUSTOMER_ACCOUNT_ID  ,  
	  incoming.ID_IPTEL_ORDER ,        
	  incoming.SOURCED_FROM_SYSTEM ,   
	  incoming.LAST_UPDATED_FROM_SYSTEM,
	  incoming.DTM_CREATED  ,          
	  incoming.DTM_EFFTV      ,        
	  incoming.DTM_EXPIRED     ,       
	  incoming.DTM_LAST_MODIFIED ,
	  incoming.DTM_LAST_UPDATED 
      
FROM 
${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} incoming
LEFT OUTER JOIN
  ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table} work
ON incoming.OV_TELEPHONE_NUMBER_ID = work.OV_TELEPHONE_NUMBER_ID
WHERE 
  work.OV_TELEPHONE_NUMBER_ID is null
UNION ALL
SELECT  
  OV_TELEPHONE_NUMBER_ID ,
  CORP	  ,
  TELEPHONE_NUMBER ,      
  AREA_CODE      ,        
  PHONE_EXCHANGE  ,       
  EXCHANGE_EXTENTION  ,   
  CUSTOMER_ACCOUNT_ID  ,  
  ID_IPTEL_ORDER ,        
  SOURCED_FROM_SYSTEM ,   
  LAST_UPDATED_FROM_SYSTEM,
  DTM_CREATED  ,          
  DTM_EFFTV      ,        
  DTM_EXPIRED     ,       
  DTM_LAST_MODIFIED ,
  DTM_LAST_UPDATED 
     
FROM  
  ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table} ) unionall
;

--##############################################################################
--#                                    End                                     #
--##############################################################################