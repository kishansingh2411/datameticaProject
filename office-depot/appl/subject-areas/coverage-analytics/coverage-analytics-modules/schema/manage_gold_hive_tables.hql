--#######################################################################################
--#                              General Details                               		    #
--#######################################################################################
--#                                                                            		    #
--# Name                                                                                #
--#     : manage_gold_hive_tables      								   	 		        #
--# File                                                                                #
--#     : manage_gold_hive_tables.hql                                   	            #
--# Description                                                                         #
--#     : Contains ddl for Hive Schema 		  									        #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--# Author                                                                              #
--#     : kishan             			 					            				#
--#                                                                                     #
--#######################################################################################

set mapred.job.name=${hiveconf:JOB_MANAGE_GOLD_SCHEMA_SSH_ACTION};
--# gold_univ
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_at}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_be}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_fr}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_ge}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_it}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_ir}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_nl}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_UNIV} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_uk}');


--# gold_cdmprddta_dm
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_at}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_be}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_fr}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_ge}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_it}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_ir}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_nl}');
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CDMPRDDTA_DM} add partition (batch_id='${hiveconf:batchId}' , country_cd='${hiveconf:CC_country_code_uk}');

--##############################################################################
--#                                    End                                     #
--##############################################################################
