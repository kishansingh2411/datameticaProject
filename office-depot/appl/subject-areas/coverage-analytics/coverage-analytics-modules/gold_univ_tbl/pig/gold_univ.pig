--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_univ            		                					   	   #
--# File                                                                       #
--#     : gold_univ.pig                                                 	   #
--# Description                                                                #
--#     : To load data into gold_univ table.                        	   	   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Shweta Karwa, Kishan singh        			   			     	   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_UNIV_SSH_ACTION';

Register $OPENCSV_JAR;

-- Loading data from work_coverage_analytics.work_fr_mdugast_n80fin table
gold_fr_mdugast_n80fin = 
   LOAD '$DB_WORK.$WORK_FR_MDUGAST_N80FIN'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_coverage_analytics.work_ge_univ_schober table
gold_ge_univ_schober = 
   LOAD '$DB_WORK.$WORK_GE_UNIV_SCHOBER'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_coverage_analytics.work_ir_univ_billmoss table
gold_ir_univ_billmoss = 
   LOAD '$DB_WORK.$WORK_IR_UNIV_BILLMOSS'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
 
-- Loading data from work_coverage_analytics.work_it_univ_dbitaly table
gold_it_univ_dbitaly = 
   LOAD '$DB_WORK.$WORK_IT_UNIV_DBITALY'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_coverage_analytics.work_nl_univ_vikbase table
gold_nl_univ_vikbase = 
   LOAD '$DB_WORK.$WORK_NL_UNIV_VIKBASE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_coverage_analytics.work_uk_univ_lbmdata table
gold_uk_univ_lbmdata = 
   LOAD '$DB_WORK.$WORK_UK_UNIV_LBMDATA'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
  
 -- Loading data from work_coverage_analytics.work_be_univ_schober table
gold_be_univ_schober = 
   LOAD '$DB_WORK.$WORK_BE_UNIV_SCHOBER'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_coverage_analytics.work_at_univ_schober table
gold_at_univ_schober = 
   LOAD '$DB_WORK.$WORK_AT_UNIV_SCHOBER'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from incoming_coverage_analytics.incoming_dim_nace table
incoming_dim_nace = 
   LOAD '$DB_INCOMING.$INCOMING_DIM_NACE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   

--##############################################################################
--##                                 Transform                                 ##
--##############################################################################

-- Filtering data for gold_fr_mdugast_n80fin current batch_id.
gold_fr_mdugast_n80fin_filter = 
   FILTER gold_fr_mdugast_n80fin BY (batch_id=='$batch_id');


-- Filter work_dim_nace for code and section   
work_dim_nace_filter = 
   FOREACH incoming_dim_nace GENERATE 
   code AS code, 
   section AS section;
   
gold_fr_mdugast_n80fin_filter_join = 
   JOIN gold_fr_mdugast_n80fin_filter BY (SUBSTRING(apet700,0,2)) LEFT OUTER, work_dim_nace_filter BY code;


-- Generating required fields from gold_fr_mdugast_n80fin table.
generate_records_fr_mdugast_n80fin = 
   FOREACH gold_fr_mdugast_n80fin_filter_join GENERATE
		siret AS id,
		l1_nomen AS company,
		codpos AS postcode,
		rpet AS region,
		efetcent AS emps_band,
		'' AS total_no_of_employees,
		section AS sic_nace_description,
		apet31 AS sic_nace_description_2,
		(SUBSTRING(apet700,0,3)) AS sic_nace_description_4,
		apet700 AS sic_nace_description_5,
		tcaexp AS turnover,
		((chararray)(efetcent=='-' OR efetcent=='' OR efetcent==' ' ? '0' : (chararray)((int)efetcent*175))) AS spend_potential,
		batch_id AS batch_id,
		country_code AS country_cd;
		
-- Filtering data for current batch_id.

gold_ge_univ_schober_filter = 
   FILTER gold_ge_univ_schober BY (batch_id=='$batch_id');

-- Generating required fields from gold_uk_univ_lbmdata table.
generate_gold_ge_univ_schober_filter = 
   FOREACH gold_ge_univ_schober_filter GENERATE
		company_id AS id,
		name_1 AS company,
		zipcode AS postcode,
		zipcode_po_box AS region,
		company_size AS emps_band,
		no_of_employees AS total_no_of_employees,
		activity_code AS sic_nace_description,
		'' AS sic_nace_description_2,
		'' AS sic_nace_description_4,
		'' AS sic_nace_description_5,
		turnover AS turnover,
		((chararray)(no_of_employees=='-' OR no_of_employees=='' OR no_of_employees==' ' ? (chararray)((int)no_of_employees*300) : (chararray)((int)no_of_employees*175))) AS spend_potential,
		batch_id AS batch_id,
		country_code AS country_code;
		
-- Filtering data for current batch_id.
gold_ir_univ_billmoss_filter = 
   FILTER gold_ir_univ_billmoss BY (batch_id=='$batch_id');

-- Generating required fields from gold_ir_univ_billmoss table.
generate_gold_ir_univ_billmoss_filter = 
   FOREACH gold_ir_univ_billmoss_filter GENERATE
		siteid AS id,
		site_name AS company,
		'' AS postcode,
		(add6=='' ? add5 : add6) AS region,
		'' AS emps_band,
		employees AS total_no_of_employees,
		'' AS sic_nace_description,
		'' AS sic_nace_description_2,
		'' AS sic_nace_description_5,
		sic1987_4digit AS sic_nace_description_4,
		'' AS turnover,
		((chararray)(employees==0 ? '0' : (chararray)((int)employees*175))) AS spend_potential,
		batch_id AS batch_id,
		country_code AS country_code;
				
-- Filtering data for current batch_id.
gold_it_univ_dbitaly_filter = 
   FILTER gold_it_univ_dbitaly BY (batch_id=='$batch_id');

-- Generating required fields from gold_it_univ_dbitaly table.
generate_gold_it_univ_dbitaly_filter = 
   FOREACH gold_it_univ_dbitaly_filter GENERATE
		record_id AS id,
		company_name AS company,
		postal_code AS postcode,
		'' AS region,
		employee_size_desc AS emps_band,
		employee_size_code AS total_no_of_employees,
		activity_group_code AS sic_nace_description,
		'' AS sic_nace_description_2,
		'' AS sic_nace_description_4,
		'' AS sic_nace_description_5,
		'' AS turnover,
		((chararray)(employee_size_code=='-' OR employee_size_code=='' OR employee_size_code==' ' ? (chararray)((int)employee_size_code*300) : (chararray)((int)employee_size_code*175))) AS spend_potential,
		batch_id AS batch_id,
		country_code AS country_code;
	
		
-- Filtering data for current batch_id.
gold_nl_univ_vikbase_filter = 
   FILTER gold_nl_univ_vikbase BY (batch_id=='$batch_id');


generate_gold_nl_univ_vikbase_filter =
   FOREACH gold_nl_univ_vikbase_filter GENERATE
   rnum AS id,
   naam AS company,
   post AS postcode,
   '' as region,
   '' as emps_band,
   bgrt AS total_no_of_employees,
   hbrn AS sic_nace_description,
   tbrn AS sic_nace_description_2,
   '' AS sic_nace_description_4,
   '' AS sic_nace_description_5,
   '' AS turnover,
   ((chararray)(bgrt == '-' OR bgrt == '' OR bgrt == ' ' ? (chararray)((int)bgrt*300) : (chararray)((int)bgrt*175))) AS spend_potential,
   batch_id AS batch_id,
   country_code AS country_code;

-- Filtering data for current batch_id.
gold_uk_univ_lbmdata_filter = 
   FILTER gold_uk_univ_lbmdata BY (batch_id=='$batch_id');

-- Generating required fields from gold_uk_univ_lbmdata table.
generate_gold_uk_univ_lbmdata_filter =
   FOREACH gold_uk_univ_lbmdata_filter GENERATE
   marketdata_urn AS id,
   company AS company,
   postcode AS postcode,
   postcoderegion AS region,
   emps_band_large AS emps_band,
   total_emps AS total_no_of_employees,
   sicsector_description AS sic_nace_description,
   sic2_description AS sic_nace_description_2,
   sic4_description AS sic_nace_description_4,
   sic5_description AS sic_nace_description_5,
   comp_turnoverband_descr AS turnover,
   ((chararray)(total_emps=='-' OR total_emps=='' OR total_emps==' ' ? '0' : (chararray)((int)total_emps*150))) AS spend_potential,
   batch_id AS batch_id,
   country_code AS country_code;
      
-- Filtering data for current batch_id.
gold_be_univ_schober_filter = 
   FILTER gold_be_univ_schober BY (batch_id=='$batch_id');

-- Generating required fields from gold_be_univ_schober table.
generate_gold_be_univ_schober_filter = 
   FOREACH gold_be_univ_schober_filter GENERATE
		be_id AS id,
		company_name AS company,
		postcode AS postcode,
		'' AS region,
		'' AS emps_band,
		employees AS total_no_of_employees,
		nace_bel AS SIC_NACE_Description,
		'' AS sic_nace_description_2,
		'' AS sic_nace_description_4,
		'' AS sic_nace_description_5,	
		turnover AS turnover,
		((chararray)(employees=='-' OR employees=='' OR employees==' ' ? (chararray)((int)employees*300) : (chararray)((int)employees*175))) AS spend_potential,
		batch_id AS batch_id,
		country_code AS country_code;
		
-- Filtering data for current batch_id.
gold_at_univ_schober_filter = 
   FILTER gold_at_univ_schober BY (batch_id=='$batch_id');


generate_gold_at_univ_schober_filter = 
   FOREACH gold_at_univ_schober_filter GENERATE
		company_id AS id,
		company_name_1 AS company,
		street_postalcode AS postcode,
		'' AS region,
		company_size AS emps_band,
		no_of_employees AS total_no_of_employees,
		nace_2008_desc AS sic_nace_description,
		'' AS sic_nace_description_2,
		'' AS sic_nace_description_4,
		'' AS sic_nace_description_5,		
		turnover AS turnover,
		((chararray)(no_of_employees=='-' OR no_of_employees=='' OR no_of_employees==' ' ? (chararray)((int)no_of_employees*300) : (chararray)((int)no_of_employees*175))) AS spend_potential,
		batch_id AS batch_id,
		country_code AS country_code;
   
--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE generate_records_fr_mdugast_n80fin
  INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_fr'
  USING parquet.pig.ParquetStorer;

STORE generate_gold_ge_univ_schober_filter
  INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_ge'
  USING parquet.pig.ParquetStorer;

STORE generate_gold_ir_univ_billmoss_filter
 INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_ir'
  USING parquet.pig.ParquetStorer;

STORE generate_gold_it_univ_dbitaly_filter
  INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_it'
  USING parquet.pig.ParquetStorer;
  
STORE generate_gold_nl_univ_vikbase_filter
  INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_nl'
  USING parquet.pig.ParquetStorer;   

STORE generate_gold_uk_univ_lbmdata_filter
  INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_uk'
  USING parquet.pig.ParquetStorer;
  
STORE generate_gold_be_univ_schober_filter
  INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_be'
  USING parquet.pig.ParquetStorer;

STORE generate_gold_at_univ_schober_filter
  INTO '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_at'
  USING parquet.pig.ParquetStorer;


--##############################################################################
--#                                   End                                      #
--##############################################################################