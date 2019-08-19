--#######################################################################################
--#                              General Details                               		    #
--#######################################################################################
--#                                                                            		    #
--# Name                                                                                #
--#     : work_cdmprddta_dm_uk          								   	 		    #
--# File                                                                                #
--#     : work_cdmprddta_dm_uk.hql                                      	            #
--# Description                                                                         #
--#     : Contains dml for loading data in work_DM_univ table					        #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--# Author                                                                              #
--#     : Shweta                		            	 					            #
--#                                                                                     #
--#######################################################################################

add jar ${hiveconf:OPENCSV_JAR};
set mapred.job.name=${hiveconf:JOB_WORK_CDMPRDDTA_DM_UK_SSH_ACTION};
INSERT INTO TABLE ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_UK}
PARTITION (batch_id = '${hiveconf:batch_id}')
SELECT
   a.COUNTRY_CD
  ,c.FISCAL_MONTH
  ,c.FISCAL_YEAR
  ,a.CUSTOMER_ID
  ,d.CUSTOMER_NM
  ,b.ASSOCIATE_ID
  ,a.ACCOUNTING_DT AS TRX_DATE
  ,case when (concat(coalesce(assc.ASSOCIATE_FIRST_NAME,''),' ',coalesce(assc.ASSOCIATE_LAST_NAME,''))) = ' ' then '${hiveconf:VARIABLE_UNAVAILABLE}'
   else (concat(coalesce(assc.ASSOCIATE_FIRST_NAME,''),' ',coalesce(assc.ASSOCIATE_LAST_NAME,''))) end AS ASSOCIATE_FULL_NAME
  ,a.TRANSACTION_ID
  ,EXT_SELLING_PRICE_AMT AS NET_SALES
  ,EXT_BIG_DEAL_COST_AMT AS BIG_DEAL
  ,EXT_GROSS_COST_AMT AS COGS
  FROM ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_TRANSACTION_DTL} a
  LEFT JOIN ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER} b
   ON (a.COUNTRY_CD = b.COUNTRY_CD
      AND a.CUSTOMER_ID = b.CUSTOMER_ID
      AND a.BUSINESS_CHANNEL_ID = b.BUSINESS_UNIT_TYPE_CD
      AND a.OD_CUSTOMER_TYPE_CD = b.OD_CUSTOMER_TYPE_CD)
  LEFT JOIN ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_CALENDAR} c
  ON (a.ACCOUNTING_DT = c.CALENDAR_DT)
  LEFT JOIN ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSOCIATE} assc
   ON (b.ASSOCIATE_ID = assc.ASSOCIATE_ID
      AND b.BUSINESS_UNIT_TYPE_CD = assc.BUSINESS_CHANNEL_ID
      AND b.COUNTRY_CD = assc.COUNTRY_CD)
  JOIN ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT} d
   ON (a.COUNTRY_CD = d.COUNTRY_CD
      AND a.BUSINESS_CHANNEL_ID = d.BUSINESS_UNIT_TYPE_CD
      AND a.OD_CUSTOMER_TYPE_CD = d.OD_CUSTOMER_TYPE_CD
      AND a.CUSTOMER_ID = d.CUSTOMER_ID)  WHERE 
  b.END_DT = '9999-12-31'
  AND a.COUNTRY_CD = 'GB'
  and a.batch_id = '${hiveconf:batch_id}'
;