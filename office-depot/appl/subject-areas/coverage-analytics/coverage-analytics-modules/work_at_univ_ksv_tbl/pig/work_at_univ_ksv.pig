--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_at_univ_ksv                                                     #
--# File                                                                       #
--#     : work_at_univ_ksv.pig                                                 #
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

SET job.name '$WORK_AT_UNIV_KSV_SSH_ACTION';

work_at_univ_ksv = 
	LOAD '$DB_INCOMING.$INCOMING_AT_UNIV_KSV'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################
work_at_univ_ksv_generate = 
   FOREACH work_at_univ_ksv GENERATE
   RTRIM(herold_id),
   RTRIM(ksv_id),
   RTRIM(name),
   RTRIM(street),
   RTRIM(zipcode),
   RTRIM(city),
   RTRIM(no_of_employees),
   RTRIM(no_white_collar_workers),
   RTRIM(no_blue_collar_workers),
   RTRIM(company_name),
   RTRIM(company_name_ksv),
   RTRIM(uid_no),
   RTRIM(telephone_1),
   RTRIM(telephone_2),
   RTRIM(telephone_3),
   RTRIM(telephone_4),
   RTRIM(telephone_5),
   RTRIM(e_mail_1),
   RTRIM(e_mail_2),
   RTRIM(e_mail_3),
   RTRIM(e_mail_4),
   RTRIM(e_mail_5),
   RTRIM(cat_branch_group_1),
   RTRIM(cat_branch_group_2),
   RTRIM(cat_branch_group_3),
   RTRIM(cat_branch_group_4),
   RTRIM(cat_branch_group_5),
   RTRIM(at_nace_branch_no_1),
   RTRIM(at_nace_branch_no_2),
   RTRIM(at_nace_branch_no_3),
   RTRIM(at_nace_branch_no_4),
   RTRIM(at_nace_branch_no_5),
   RTRIM(at_nace_branch_1),
   RTRIM(at_nace_branch_2),
   RTRIM(at_nace_branch_3),
   RTRIM(at_nace_branch_4),
   RTRIM(at_nace_branch_5);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_at_univ_ksv_generate 
	INTO '$WORK_HDFS/$WORK_AT_UNIV_KSV/batch_id=$batch_id/country_code=$country_code_at'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################       