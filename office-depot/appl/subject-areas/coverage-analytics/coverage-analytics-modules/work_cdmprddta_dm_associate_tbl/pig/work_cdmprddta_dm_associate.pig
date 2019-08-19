--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cdmprddta_dm_associate                                     #
--# File                                                                       #
--#     : work_cdmprddta_dm_associate.pig                                  #
--# Description                                                                #
--#     : To load data into work layer                                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Sonali Rawool                                                         #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################
Register $OPENCSV_JAR;

SET job.name '$WORK_CDMPRDDTA_DM_ASSOCIATE_SSH_ACTION';

work_cdmprddta_dm_associate = 
	LOAD '$DB_INCOMING.$INCOMING_CDMPRDDTA_DM_ASSOCIATE'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################
work_cdmprddta_dm_associate_generate = 
	FOREACH work_cdmprddta_dm_associate GENERATE
	RTRIM(associate_id),
	RTRIM(business_channel_id),
	RTRIM(associate_first_name),
	RTRIM(associate_last_name),
	RTRIM(job_title_txt),
	RTRIM(associate_start_dt),
	RTRIM(associate_end_dt),
	RTRIM(manager_associate_id),
	RTRIM(country_cd),
	RTRIM(associate_status_ind),
	RTRIM(associate_email),
	RTRIM(associate_gender_cd),
	RTRIM(associate_cost_centre),
	RTRIM(associate_type_cd),
	RTRIM(associate_peoplesoft_id),
	RTRIM(associate_position_cd),
	RTRIM(associate_org_unit),
	RTRIM(processed_dt);

--##############################################################################
--#                                   Store                                    #
--##############################################################################
STORE work_cdmprddta_dm_associate_generate 
	INTO '$WORK_HDFS/$WORK_CDMPRDDTA_DM_ASSOCIATE/batch_id=$batch_id/'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################