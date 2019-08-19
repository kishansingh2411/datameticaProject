--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_ge_univ_schober                                      		   #
--# File                                                                       #
--#     : work_ge_univ_schober.pig                                  		   #
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

SET job.name '$WORK_GE_UNIV_SCHOBER_SSH_ACTION';

work_ge_univ_schober = 
	LOAD '$DB_INCOMING.$INCOMING_GE_UNIV_SCHOBER'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_ge_univ_schober_generate = 
	FOREACH work_ge_univ_schober GENERATE
	RTRIM(company_id),
	RTRIM(name_1),
	RTRIM(name_2),
	RTRIM(name_3),
	RTRIM(zipcode),
	RTRIM(town),
	RTRIM(street),
	RTRIM(po_box),
	RTRIM(zipcode_po_box),
	RTRIM(town_po_box),
	RTRIM(company_salutation),
	RTRIM(tel_area_code),
	RTRIM(telephone_no),
	RTRIM(fax_area_code),
	RTRIM(fax_no),
	RTRIM(legal_form),
	RTRIM(company_size),
	RTRIM(company_type),
	RTRIM(year_of_foundation),
	RTRIM(chamber_of_commerce_no),
	RTRIM(no_of_employees),
	RTRIM(turnover),
	RTRIM(population),
	RTRIM(capital),
	RTRIM(homepage),
	RTRIM(email),
	RTRIM(mobile_phone_no),
	RTRIM(service_phone_no),
	RTRIM(region),
	RTRIM(mailorder_activity),
	RTRIM(employee_category),
	RTRIM(activity_code),
	RTRIM(no_white_collar_workers),
	RTRIM(no_blue_collar_workers);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_ge_univ_schober_generate 
	INTO '$WORK_HDFS/$WORK_GE_UNIV_SCHOBER/batch_id=$batch_id/country_code=$country_code_ge'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################  