--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_cust_services_last_rec_merge_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 05/02/2016
--#   Log File    : 
--#   SQL File    : 
--#   Error File  : 
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/02/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.compress.output=true;
set avro.output.codec=snappy;

INSERT INTO TABLE 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table}   
SELECT
		fltr_con.ID_SERVICE_REC,
		fltr_con.HOME_ID,             
		fltr_con.ACCOUNT_NUMBER ,            
		fltr_con.ID_SERVICE_TYPE,              
		fltr_con.ID_SYS_INFO  ,                
		fltr_con.ID_STATUS   ,                 
		fltr_con.CH_HOME_PH  ,                 
		fltr_con.CH_WORK_PH ,                  
		fltr_con.CH_EMAIL_ADD  ,               
		fltr_con.CH_USERNAME ,                 
		fltr_con.CH_PASSWD  ,                  
		fltr_con.DT_START_DATE,                
		fltr_con.DT_END_DATE ,                 
		fltr_con.DT_LAS_MOD_DATE ,             
		fltr_con.CH_HOMEAREA_CD ,              
		fltr_con.CH_RATE   ,                   
		fltr_con.CH_BDATE   ,                  
		fltr_con.CH_BILLTHRUDATE ,             
		fltr_con.CH_CYCLE ,                    
		fltr_con.CH_DRSN  ,                    
		fltr_con.CH_HOLD ,                     
		fltr_con.CH_COLLECT_STATUS   ,         
		fltr_con.CH_ACCT_STATUS_QUALIFIER ,    
		fltr_con.DT_ACCT_STATUS_QUALIFIER_DATE,
		fltr_con.DT_STATUS_DATE  ,             
		fltr_con.OP_TYPE  ,                    
		fltr_con.LOAD_DATE  ,                  
		fltr_con.CUST_SERVICES_DELTA_SEQ  
FROM 
(
SELECT 
	*, dense_rank() over( partition by (join_con.home_id) 
ORDER BY join_con.id_service_rec desc
) AS dense_value 
FROM
		(SELECT 
			src_tbl.ID_SERVICE_REC,
			src_tbl.HOME_ID,             
			src_tbl.ACCOUNT_NUMBER ,            
			src_tbl.ID_SERVICE_TYPE,              
			src_tbl.ID_SYS_INFO  ,                
			src_tbl.ID_STATUS   ,                 
			src_tbl.CH_HOME_PH  ,                 
			src_tbl.CH_WORK_PH ,                  
			src_tbl.CH_EMAIL_ADD  ,               
			src_tbl.CH_USERNAME ,                 
			src_tbl.CH_PASSWD  ,                  
			src_tbl.DT_START_DATE,                
			src_tbl.DT_END_DATE ,                 
			src_tbl.DT_LAS_MOD_DATE ,             
			src_tbl.CH_HOMEAREA_CD ,              
			src_tbl.CH_RATE   ,                   
			src_tbl.CH_BDATE   ,                  
			src_tbl.CH_BILLTHRUDATE ,             
			src_tbl.CH_CYCLE ,                    
			src_tbl.CH_DRSN  ,                    
			src_tbl.CH_HOLD ,                     
			src_tbl.CH_COLLECT_STATUS   ,         
			src_tbl.CH_ACCT_STATUS_QUALIFIER ,    
			src_tbl.DT_ACCT_STATUS_QUALIFIER_DATE,
			src_tbl.DT_STATUS_DATE  ,             
			src_tbl.OP_TYPE  ,                    
			src_tbl.LOAD_DATE  ,                  
			src_tbl.CUST_SERVICES_DELTA_SEQ 
FROM 
         ${hivevar:source_database1}.${hivevar:table_prefix}${hivevar:source_table1} src_tbl
         
FULL OUTER JOIN 
         ${hivevar:source_database2}.${hivevar:table_prefix}${hivevar:source_table2} gld_tbl
ON
          src_tbl.ID_SERVICE_REC = gld_tbl.ID_SERVICE_REC 
WHERE
           gld_tbl.ID_SERVICE_REC is NULL AND 
           src_tbl.SOURCE_DATE='${hivevar:source_date}'
UNION
SELECT 
           gld_tbl.* 
FROM
  		 ${hivevar:source_database2}.${hivevar:table_prefix}${hivevar:source_table2} gld_tbl
)join_con ) fltr_con
WHERE fltr_con.dense_value=1;

--##############################################################################
--#                                    End                                     #
--##############################################################################