--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the outgoing outgoing_export_cmts_node_tbl table from gold.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 02/12/2017
--#   Log File    : .../log/footprints/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/footprints/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          02/12/2017       Initial version
--#
--#
--#####################################################################################################################

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

set hive.exec.compress.output=false; 

INSERT OVERWRITE DIRECTORY '${hivevar:dir_path}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' 
SELECT 
	BEGIN_DATE             
	,END_DATE               
	,REGION_PS_ID           
	,PLANT_MANAGER_DESCR    
	,HEAD_END_DESCR         
	,NODE_CD                
	,ITEM_NBR               
	,EQMT_TYPE              
	,CUST_COUNT             
	,EQMT_COUNT             
	,IVR_TECH_TOTAL         
	,IVR_TECH_CONTAIN       
	,IVR_TECH_OFFER         
	,IVR_TECH_VIDEO_TOTAL   
	,IVR_TECH_VIDEO_CONTAIN 
	,IVR_TECH_VIDEO_OFFER   
	,IVR_TECH_DATA_TOTAL    
	,IVR_TECH_DATA_CONTAIN  
	,IVR_TECH_DATA_OFFER    
	,IVR_TECH_PHONE_TOTAL   
	,IVR_TECH_PHONE_CONTAIN 
	,IVR_TECH_PHONE_OFFER   
	,WO_TC_VIDEO            
	,WO_TC_DATA             
	,WO_TC_PHONE            
	,WO_TC_TOTAL            
	,MODEL                  
	,MODEL_DESC             
	,LOB                    
	,CATEGORY               
	,MAKE                   
	,ACTIVE                 
	,RYG_STATUS             
	,CPE_FORECAST_CATEGORY  
	,M2_OR_M4               
	,MODEL_TRIM             
	,MODEL_CATEGORY         
	,DOCSIS 
FROM 
	${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_cpe_weekly_table}
WHERE 
	P_YYYYMMDD='${hivevar:current_week}'     

--##############################################################################
--#                                    End                                     #
--##############################################################################