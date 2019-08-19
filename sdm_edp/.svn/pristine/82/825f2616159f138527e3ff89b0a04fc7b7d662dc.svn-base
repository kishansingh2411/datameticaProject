--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the gold gold_equipment_dtls_tbl table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
--#   Date        : 1/18/2017
--#   Log File    : .../log/sdm/${job_name}.log
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/22/2017
--#   SQL File    : 
--#   Error File  : .../log/sdm/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          1/18/2017      Initial version
--#
--#
--#####################################################################################################################


INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
  PARTITION(P_YYYYMMDD)
SELECT 
SITE_ID                  
,NVL(ITEM_NBR,'null')                 
,NVL(SERIAL_NBR,'null')               
,PORT_NBR                 
,NVL(EQUIPMENT_ADDRESSABLE,'null')    
,STATUS_DT                
,NVL(QUALITY_ASSURANCE_CD,'null')     
,QUALITY_ASSURANCE_DT     
,NVL(EQUIPMENT_ADDRESS,'null') 
,NVL(MAC_ADDRESS,'null')      
,NVL(EQUIPMENT_OVERRIDE_ACTIVE,'null')
,NVL(INITIALIZE_REQUIRED,'null')      
,NVL(PARENTAL_CD,'null')              
,NVL(TEMP_ENABLED,'null')             
,TRANSMISSION_DT          
,NVL(DNS_NM,'null')                  
,NVL(IP_ADDRESS,'null')               
,NVL(FQDN,'null')                    
,NVL(LOCAL_STATUS,'null')             
,LOCAL_STATUS_DT          
,NVL(SERVER_ID,'null')                
,NVL(SERVER_STATUS,'null')            
,SERVER_STATUS_DT         
,NVL(EQUIP_DTL_STATUS,'null')         
,NVL(PORT_CAT_CD,'null')              
,NVL(CABLE_CARD_ID,'null')            
,NVL(HEADEND,'null')                 
,ACCT_NBR                 
,SUB_ACCT_ID              
,NVL(VIDEO_RATING_CD,'null')          
,NVL(PORT_TYPE,'null')                
,NVL(SERVICE_CAT_CD,'null')           
,SERVICE_OCCURRENCE       
,NVL(CREATED_USER_ID,'null')          
,DATE_CREATED             
,NVL(LAST_CHANGE_USER_ID,'null')      
,LAST_CHANGE_DT           
,EQUIPMENT_KEY            
,equipment_dtls_KEY       
,CUSTOMER_KEY             
,ITEM_DTL_KEY  
,P_YYYYMMDD
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
WHERE P_YYYYMMDD='${hivevar:source_date}'

--##############################################################################
--#                                    End                                     #
--##############################################################################