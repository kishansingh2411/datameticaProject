--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_be_univ_schober                                                 #
--# File                                                                       #
--#     : work_be_univ_schober.pig                                             #
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

SET job.name '$WORK_BE_UNIV_SCHOBER_SSH_ACTION';

work_be_univ_schober = 
	LOAD '$DB_INCOMING.$INCOMING_BE_UNIV_SCHOBER'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################
work_be_univ_schober_generate = 
   FOREACH work_be_univ_schober GENERATE
   RTRIM(be_id),
   RTRIM(be_code),
   RTRIM(btw_number),
   RTRIM(company_name),
   RTRIM(legal_form),
   RTRIM(street),
   RTRIM(house_number),
   RTRIM(house_number_add),
   RTRIM(postcode),
   RTRIM(city),
   RTRIM(legal_district),
   RTRIM(province),
   RTRIM(language),
   RTRIM(nace_bel),
   RTRIM(main_activity_code),
   RTRIM(activity_code2),
   RTRIM(activity_code3),
   RTRIM(activity_code4),
   RTRIM(activity_code5),
   RTRIM(phone_number),
   RTRIM(fax_number),
   RTRIM(employee_size),
   RTRIM(date_of_foundation),
   RTRIM(ceo_name),
   RTRIM(ceo_first_name),
   RTRIM(ceo_sex),
   RTRIM(ceo_language),
   RTRIM(mng_dir_name),
   RTRIM(mng_dir_first_name),
   RTRIM(mng_dir_sex),
   RTRIM(mng_dir_language),
   RTRIM(internal_schober),
   RTRIM(mng_pur),
   RTRIM(mng_pur_first_name),
   RTRIM(mng_pur_sex),
   RTRIM(mng_pur_language),
   RTRIM(mng_hr),
   RTRIM(mng_hr_first_name),
   RTRIM(mng_hr_sex),
   RTRIM(mng_hr_language),
   RTRIM(mng_it),
   RTRIM(mng_it_first_name),
   RTRIM(mng_it_sex),
   RTRIM(mng_it_language),
   RTRIM(dir_sales),
   RTRIM(dir_sales_first_name),
   RTRIM(dir_sales_sex),
   RTRIM(dir_sales_language),
   RTRIM(mng_mkt),
   RTRIM(mng_mkt_first_name),
   RTRIM(mng_mkt_sex),
   RTRIM(mng_mkt_language),
   RTRIM(dir_financ),
   RTRIM(dir_financ_first_name),
   RTRIM(dir_financ_sex),
   RTRIM(dir_financ_language),
   RTRIM(email),
   RTRIM(website),
   RTRIM(turnover),
   RTRIM(import1),
   RTRIM(export),
   RTRIM(company_type),
   RTRIM(employees),
   RTRIM(white_collar_class),
   RTRIM(ebi_number),
   RTRIM(filler);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_be_univ_schober_generate 
	INTO '$WORK_HDFS/$WORK_BE_UNIV_SCHOBER/batch_id=$batch_id/country_code=$country_code_be'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################
