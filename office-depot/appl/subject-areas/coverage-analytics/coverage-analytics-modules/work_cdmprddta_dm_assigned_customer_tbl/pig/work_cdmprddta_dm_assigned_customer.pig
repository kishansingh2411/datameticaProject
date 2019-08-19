--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cdmprddta_dm_assigned_customer                                  #
--# File                                                                       #
--#     : work_cdmprddta_dm_assigned_customer.pig                              #
--# Description                                                                #
--#     : To load data into work layer                                         #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Sonali Rawool                                                        #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################
Register $OPENCSV_JAR;

SET job.name '$WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER_SSH_ACTION';

work_cdmprddta_dm_assigned_customer = 
	Load '$DB_INCOMING.$INCOMING_CDMPRDDTA_DM_ASSIGNED_CUSTOMER'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################
work_cdmprddta_dm_assigned_customer_generate = 
	FOREACH work_cdmprddta_dm_assigned_customer GENERATE
	RTRIM(country_cd),
	RTRIM(business_unit_type_cd),
	RTRIM(od_customer_type_cd),
	RTRIM(associate_id),
	RTRIM(customer_id),
	RTRIM(start_dt),
	RTRIM(end_dt),
	RTRIM(processed_dt);

--##############################################################################
--#                                   Store                                    #
--##############################################################################
STORE work_cdmprddta_dm_assigned_customer_generate 
	INTO '$WORK_HDFS/$WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER/batch_id=$batch_id/'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################

         