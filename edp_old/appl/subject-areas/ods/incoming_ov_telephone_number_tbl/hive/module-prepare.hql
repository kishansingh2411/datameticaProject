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

INSERT INTO TABLE 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} 
SELECT 
    OV_TELEPHONE_NUMBER_ID  ,
	CORP         ,           
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
FROM ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################