--#######################################################################################
--#                              General Details                               		    #
--#######################################################################################
--#                                                                            		    #
--# Name                                                                                #
--#     : manage_work_hive_tables      								   	 		        #
--# File                                                                                #
--#     : manage_work_hive_tables.hql                                   	            #
--# Description                                                                         #
--#     : Contains ddl for Hive Schema 		  									        #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--# Author                                                                              #
--#     : Sonali Rawool             			 					                    #
--#                                                                                     #
--#######################################################################################

set mapred.job.name=${hiveconf:JOB_MANAGE_WORK_SCHEMA_SSH_ACTION};
--# work_univ
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_NL_UNIV_VIKBASE} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_nl}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_GE_UNIV_SCHOBER} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_ge}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_BE_UNIV_SCHOBER} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_be}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_IT_UNIV_DBITALY} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_it}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_AT_UNIV_KSV} add partition (batch_id='${hiveconf:batchId}' , country_code='${hiveconf:CC_country_code_at}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_AT_UNIV_SCHOBER} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_at}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_UK_UNIV_LBMDATA} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_uk}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_IR_UNIV_BILLMOSS} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_ir}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_FR_MDUGAST_N80FIN} add partition (batch_id='${hiveconf:batchId}', country_code='${hiveconf:CC_country_code_fr}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER} add partition (batch_id='${hiveconf:batchId}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_CDMPRDDTA_DM_TRANSACTION_DTL} add partition (batch_id='${hiveconf:batchId}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSOCIATE} add partition (batch_id='${hiveconf:batchId}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_CDMPRDDTA_DM_COUNTRY} add partition (batch_id='${hiveconf:batchId}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_CDMPRDDTA_DM_CALENDAR} add partition (batch_id='${hiveconf:batchId}');
use ${hiveconf:DB_WORK}; alter table ${hiveconf:TBL_WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT} add partition (batch_id='${hiveconf:batchId}');

--##############################################################################
--#                                    End                                     #
--##############################################################################
