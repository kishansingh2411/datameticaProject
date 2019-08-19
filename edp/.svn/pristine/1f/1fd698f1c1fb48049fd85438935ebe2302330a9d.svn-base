--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build incoming table from work table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-prepare.hql
--#   Date        : 12/28/2016
--#   Log File    : .../log/ods/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/ods/${job_name}.log
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

set hive.vectorized.execution.enabled=false;

DELETE FROM 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table}
WHERE
   CUSTOMER_ACCOUNT_ID 
      IN (
            SELECT 
               DISTINCT CUSTOMER_ACCOUNT_ID
            FROM  
               ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table}
         )
;

INSERT INTO TABLE
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table}
SELECT
   CUSTOMER_ACCOUNT_ID       
   ,DWELLING_ID               
   ,CUSTOMER_ID               
   ,CORP_ID                   
   ,CORP                      
   ,DWELLING_NBR              
   ,CUST                      
   ,STATUS                    
   ,TITLE                     
   ,LAST_NAME                 
   ,FIRST_MIDDLE_INITIAL      
   ,FIRST_NAME                
   ,ACCOUNT_TYPE              
   ,CONVERTER_IND             
   ,MONTHLY_RATE              
   ,BILLING_CYCLE             
   ,DTM_ORIGINAL_INSTALL      
   ,WRT_OFF_AMT               
   ,DTM_BILLTHRU              
   ,SALES_REASON_ID           
   ,ACTIVE_IND                
   ,CUSTOMER_ACCOUNT_XREF_ID  
   ,DTM_CUSTOMER_ACCOUNT_XREF 
   ,XREF_REASON_ID            
   ,ID_CUST                   
   ,VOD_CUSTOMER_ACCOUNT_ID   
   ,DTM_EFFTV                 
   ,DTM_EXPIRED               
   ,DTM_CREATED               
   ,DTM_LAST_UPDATE           
   ,DTM_EFFTV_ALT             
   ,DTM_EXPIRED_ALT           
   ,ACCT_INFO_CODE            
   ,ACCT_INFO_ID              
   ,ACCT_MAIL_CODE            
   ,ACCT_MAIL_ID              
   ,ORIGINAL_CAMPAIGN_CODE    
   ,ORIGINAL_CAMPAIGN_ID      
   ,ACCT_CLASS_CODE           
   ,ACCT_CLASS_ID             
   ,BILLING_ADDRESS_ID        
   ,DISCOUNT_CODE             
   ,SALES_REASON_CODE         
   ,HOLD_NBR                  
   ,BILLING_ADDRESS_CODE      
   ,ACCT_TYPE_ID              
   ,DISCOUNT_CODE_ID          
   ,IN_CARE_OF                
   ,LAST_UOW_ID               
   ,SSN                       
   ,MEMO_CREDIT               
   ,RESIDENTIAL_AREA_CODE     
   ,RESIDENTIAL_PHONE_NBR     
   ,BUSINESS_AREA_CODE        
   ,BUSINESS_PHONE_NBR        
   ,AR_BAL_CURRENT_AMT        
   ,AR_BAL_30                 
   ,AR_BAL_60                 
   ,AR_BAL_90                 
   ,AR_BAL_120                
   ,COLLECTION_STATUS         
   ,EXEMPT_FROM_COLLECTION_IND
   ,DT_CHARGED_THRU           
   ,STATEMENT_FREQ_CODE       
   ,FINANCIAL_REMINDER_SCHEME 
   ,TWILIGHT_IND              
   ,UNIQUE_CUST_IDENTIFICATION
   ,DT_OF_UNIQUE_CUSTOMER_RQST
FROM
${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################