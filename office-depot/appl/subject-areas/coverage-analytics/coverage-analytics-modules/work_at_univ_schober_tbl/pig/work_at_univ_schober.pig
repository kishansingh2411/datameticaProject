--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_at_univ_schober                                                 #
--# File                                                                       #
--#     : work_at_univ_schober.pig                                             #
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

SET job.name '$WORK_AT_UNIV_SCHOBER_SSH_ACTION';

work_at_univ_schober = 
	LOAD '$DB_INCOMING.$INCOMING_AT_UNIV_SCHOBER'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################
work_at_univ_schober_generate = FOREACH work_at_univ_schober GENERATE
   RTRIM(company_id),
   RTRIM(salutation),
   RTRIM(title),
   RTRIM(first_name),
   RTRIM(last_name),
   RTRIM(company_name_1),
   RTRIM(company_name_2),
   RTRIM(company_name_3),
   RTRIM(street),
   RTRIM(street_postalcode),
   RTRIM(town_po_box),
   RTRIM(po_box),
   RTRIM(po_box_postalcode),
   RTRIM(po_box_city),
   RTRIM(phone_area_code),
   RTRIM(phone_number),
   RTRIM(fax_area_code),
   RTRIM(fax_number),
   RTRIM(type_of_company),
   RTRIM(legal_form),
   RTRIM(no_of_employees),
   RTRIM(company_size),
   RTRIM(no_of_beds),
   RTRIM(no_chamber_of_commerce),
   RTRIM(year_foundation),
   RTRIM(business_capital),
   RTRIM(turnover),
   RTRIM(size_of_city),
   RTRIM(ebc_code),
   RTRIM(email),
   RTRIM(homepage),
   RTRIM(shared_practice),
   RTRIM(mobilphone),
   RTRIM(typ_wgs84),
   RTRIM(x_geo_wgs84),
   RTRIM(y_geo_wgs84),
   RTRIM(typ_lambert),
   RTRIM(x_geo_lambert_konf),
   RTRIM(y_geo_lambert_konf),
   RTRIM(nace_2008),
   RTRIM(nace_2008_desc),
   RTRIM(blue_collar_worker),
   RTRIM(white_collar_worker),
   RTRIM(growth_indicator),
   RTRIM(flag_score);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_at_univ_schober_generate 
	INTO '$WORK_HDFS/$WORK_AT_UNIV_SCHOBER/batch_id=$batch_id/country_code=$country_code_at'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################
         