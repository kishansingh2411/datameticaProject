--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : outgoing_view_monthly_ir                                             #
--# File                                                                       #
--#     : outgoing_view_monthly_ir.pig                                         #
--# Description                                                                #
--#     : To load data into outgoing layer                                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Shweta Karwa                                                         #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$OUTGOING_SCHEMA_IR_SSH_ACTION';
SET default_parallel 2;

-- Loading data from gold_coverage_analytics.gold_univ_tbl table.

gold_univ_tbl = LOAD '$GOLD_HDFS/$GOLD_UNIV/batch_id=$batch_id/country_cd=$country_code_ir' 
               USING parquet.pig.ParquetLoader AS (id : chararray ,company: chararray,postcode: chararray,region: chararray,emps_band: chararray,total_no_of_employees: chararray,sic_nace_description: chararray,sic_nace_description_2: chararray,sic_nace_description_4: chararray,sic_nace_description_5: chararray,turnover: chararray,spend_potential: chararray,batch_id: chararray,country_cd: chararray); 
   
-- Loading data from gold_coverage_analytics.gold_cdmprddta_dm_tbl table.

gold_cdmprddta_dm_tbl =
   LOAD '$DB_GOLD.$GOLD_CDMPRDDTA_DM'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

gold_country =
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_COUNTRY'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
  
--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.

gold_cdmprddta_dm_tbl_filter =
   FILTER gold_cdmprddta_dm_tbl BY (batch_id == '$batch_id' AND country_cd == '$country_code_ir');

-- ---------------------total_prospects--------------------------
   
gold_univ_tbl_filter_country_cd =
   FOREACH gold_univ_tbl GENERATE 
   $0 as id,
   $1 as company,
   $2 as postcode,
   $3 as region,
   $4 as emps_band,
   $5 as total_no_of_employees,
   $6 as sic_nace_description,
   $7 as sic_nace_description_2,
   $8 as sic_nace_description_4,
   $9 as sic_nace_description_5,
   $10 as turnover,
   $11 as spend_potential,
   $12 as batch_id,
   $13 as country_cd;
   
gold_univ_tbl_filter_country_cd_join =
   JOIN gold_univ_tbl_filter_country_cd BY country_cd LEFT OUTER, gold_country BY country_cd;

gold_univ_tbl_filter_country_cd_join_group = 
   GROUP gold_univ_tbl_filter_country_cd_join by gold_univ_tbl_filter_country_cd::country_cd;

gold_univ_tbl_filter_country_cd_join_group_id = 
   FOREACH gold_univ_tbl_filter_country_cd_join_group { 
			unique_id = DISTINCT gold_univ_tbl_filter_country_cd_join.id;
			GENERATE group as country_cd,COUNT(unique_id) as tot_prospects; }; 			
	
-- ------------------------Sales Representatives, Accounts, Revenue-----------------------

gold_cdmprddta_dm_tbl_filter_month =
   FOREACH gold_cdmprddta_dm_tbl_filter GENERATE
   ((int)fiscal_month <10 ? CONCAT('0',fiscal_month) : fiscal_month ) as fiscal_month,
   fiscal_year,
   customer_id,
   REPLACE(customer_nm,'([^0-9a-zA-Z\\s]+)','') as customer_nm,
   associate_id,
   REPLACE(associate_full_name,'([^0-9a-zA-Z\\s]+)','') as associate_full_name,
   tot_revenue,
   big_deal,
   cogs,
   batch_id,
   country_cd;

dm_fields_country_join =
   JOIN gold_cdmprddta_dm_tbl_filter_month BY country_cd LEFT OUTER, gold_country BY country_cd;

dm_fields_prospects_join =
   JOIN dm_fields_country_join BY gold_cdmprddta_dm_tbl_filter_month::country_cd LEFT OUTER, gold_univ_tbl_filter_country_cd_join_group_id BY country_cd;

salesrep_accounts_pros_revenue = 
   FOREACH dm_fields_prospects_join GENERATE  RTRIM(CONCAT(RTRIM((chararray)dm_fields_country_join::gold_cdmprddta_dm_tbl_filter_month::fiscal_year),(chararray)dm_fields_country_join::gold_cdmprddta_dm_tbl_filter_month::fiscal_month)) as month,
  RTRIM(dm_fields_country_join::gold_country::country_nm) as country,
   RTRIM(dm_fields_country_join::gold_cdmprddta_dm_tbl_filter_month::associate_id) as sales_represntative_id,
   RTRIM(dm_fields_country_join::gold_cdmprddta_dm_tbl_filter_month::customer_id) as account_id,
   (RTRIM((chararray)gold_univ_tbl_filter_country_cd_join_group_id::tot_prospects) IS NULL ?'0' : RTRIM((chararray)gold_univ_tbl_filter_country_cd_join_group_id::tot_prospects)) as total_prospect,
   RTRIM((chararray)dm_fields_country_join::gold_cdmprddta_dm_tbl_filter_month::tot_revenue) as total_revenue,
   RTRIM(dm_fields_country_join::gold_cdmprddta_dm_tbl_filter_month::associate_full_name) as sales_represntative_name,
   RTRIM(dm_fields_country_join::gold_cdmprddta_dm_tbl_filter_month::customer_nm) as account_name;
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into /office_depot/coverage_analytics/outgoing/daily/batch_id/country_cd directory.
STORE salesrep_accounts_pros_revenue
   INTO '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_ir'
   USING PigStorage(',');

--##############################################################################
--#                                    End                                     #
--##############################################################################

         