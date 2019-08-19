--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_uk_univ_lbmdata                                                 #
--# File                                                                       #
--#     : work_uk_univ_lbmdata.pig                                             #
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

SET job.name '$WORK_UK_UNIV_LBMDATA_SSH_ACTION';

work_uk_univ_lbmdata = 
	LOAD '$DB_INCOMING.$INCOMING_UK_UNIV_LBMDATA'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_uk_univ_lbmdata_generate = 
	FOREACH work_uk_univ_lbmdata GENERATE
	RTRIM(lbm_diallerid),
	RTRIM(marketdata_urn),
	RTRIM(company),
	RTRIM(add1),
	RTRIM(add2),
	RTRIM(add3),
	RTRIM(add4),
	RTRIM(town),
	RTRIM(county),
	RTRIM(postcode),
	RTRIM(pcarea),
	RTRIM(pcdist),
	RTRIM(postcoderegion),
	RTRIM(telephone),
	RTRIM(fax),
	RTRIM(emps),
	RTRIM(emps_band),
	RTRIM(emps_band_large),
	RTRIM(total_emps),
	RTRIM(total_emps_band_large),
	RTRIM(sic5),
	RTRIM(sic5_description),
	RTRIM(sic4),
	RTRIM(sic4_description),
	RTRIM(sic2),
	RTRIM(sic2_description),
	RTRIM(sicsector),
	RTRIM(sicsector_description),
	RTRIM(thomson_code),
	RTRIM(thomson_code_description),
	RTRIM(lbm5),
	RTRIM(lbm5_description),
	RTRIM(sitetype),
	RTRIM(no_sites),
	RTRIM(tps),
	RTRIM(tpsc),
	RTRIM(fps),
	RTRIM(mps),
	RTRIM(estdate),
	RTRIM(lbm_businesstype),
	RTRIM(lbm_businesstype_description),
	RTRIM(company_verifieddate),
	RTRIM(companylevel_turnoverband),
	RTRIM(comp_turnoverband_descr),
	RTRIM(recency_18mths),
	RTRIM(recency_12mths),
	RTRIM(months_recency),
	RTRIM(siteparentid),
	RTRIM(lbm_siteparentdiallerid),
	RTRIM(update_type),
	RTRIM(last_update_lbm),
	RTRIM(edw_load_date),
	RTRIM(edw_amend_date),
	RTRIM(sic2_2007),
	RTRIM(sic2_2007_desc),
	RTRIM(sic3_2007),
	RTRIM(sic3_2007_desc),
	RTRIM(sic4_2007),
	RTRIM(sic4_2007_desc),
	RTRIM(sic5_2007),
	RTRIM(sic5_2007_desc);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_uk_univ_lbmdata_generate 
	INTO '$WORK_HDFS/$WORK_UK_UNIV_LBMDATA/batch_id=$batch_id/country_code=$country_code_uk'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################  