--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_it_univ_dbitaly                                      		   #
--# File                                                                       #
--#     : work_it_univ_dbitaly.pig                                             #
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

SET job.name '$WORK_IT_UNIV_DBITALY_SSH_ACTION';

work_it_univ_dbitaly = 
	LOAD '$DB_INCOMING.$INCOMING_IT_UNIV_DBITALY'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_it_univ_dbitaly_generate = 
	FOREACH work_it_univ_dbitaly GENERATE
	RTRIM(record_id),
	RTRIM(progressive_record_id),
	RTRIM(company_name),
	RTRIM(surname),
	RTRIM(name),
	RTRIM(title),
	RTRIM(address),
	RTRIM(city_town_description),
	RTRIM(province_code),
	RTRIM(postal_code),
	RTRIM(sex_code),
	RTRIM(telephone_area_code),
	RTRIM(telephone_number),
	RTRIM(movement_type),
	RTRIM(elaboration_date),
	RTRIM(activity_group_code),
	RTRIM(company_registered_status),
	RTRIM(company_typecode),
	RTRIM(yellowpages_category_code),
	RTRIM(yellowpages_description),
	RTRIM(additional_activity_descr),
	RTRIM(flag_cotel),
	RTRIM(flag_copos),
	RTRIM(flag_primary_secondary),
	RTRIM(number_of_telephone_lines),
	RTRIM(region_code_istat),
	RTRIM(province_code_istat),
	RTRIM(city_town_code_istat),
	RTRIM(street_code_seat),
	RTRIM(viking_terretorial_code),
	RTRIM(vat_number),
	RTRIM(activity_date),
	RTRIM(employee_size_code),
	RTRIM(employee_size_desc),
	RTRIM(branch),
	RTRIM(fiscal_code),
	RTRIM(scoring_light),
	RTRIM(changed_town_code),
	RTRIM(changed_town_description),
	RTRIM(changed_zip_code),
	RTRIM(changed_street_code),
	RTRIM(changed_address),
	RTRIM(changed_comp_reg),
	RTRIM(changed_phone_number),
	RTRIM(changed_phone_lines),
	RTRIM(changed_yellowpages_code),
	RTRIM(changed_add_activity_descr),
	RTRIM(changed_employee_size),
	RTRIM(changed_scoring_light),
	RTRIM(changed_sede_principal),
	RTRIM(changed_vat_number),
	RTRIM(changed_fiscal_code),
	RTRIM(telephone_id),
	RTRIM(registration_date);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_it_univ_dbitaly_generate 
	INTO '$WORK_HDFS/$WORK_IT_UNIV_DBITALY/batch_id=$batch_id/country_code=$country_code_it'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################