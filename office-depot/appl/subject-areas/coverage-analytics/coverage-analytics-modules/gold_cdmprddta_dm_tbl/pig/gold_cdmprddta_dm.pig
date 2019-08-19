--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_cdmprddta_dm                								   	   #
--# File                                                                       #
--#     : gold_cdmprddta_dm.pig                                          	   #
--# Description                                                                #
--#     : To load data into gold_cdmprddta_dm table.                   	   	   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Shweta Karwa                           			   				   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_CDMPRDDTA_DM_SSH_ACTION';

-- Loading data from work_coverage_analytics.WORK_DM_FR_univ table
work_DM_FR_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_FR'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_coverage_analytics.work_DM_NL_univ table
work_DM_NL_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_NL'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_coverage_analytics.work_DM_GE_univ table
work_DM_GE_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_GE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_coverage_analytics.work_DM_BE_univ table
work_DM_BE_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_BE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_coverage_analytics.work_DM_IT_univ table
work_DM_IT_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_IT'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_coverage_analytics.work_DM_AT_univ table
work_DM_AT_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_AT'
   USING org.apache.hive.hcatalog.pig.HCatLoader();            

-- Loading data from work_coverage_analytics.work_DM_UK_univ table
work_DM_UK_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_UK'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
      
-- Loading data from work_coverage_analytics.WORK_DM_IR_univ table
work_DM_IR_univ = 
   LOAD '$DB_WORK.$WORK_CDMPRDDTA_DM_IR'
   USING org.apache.hive.hcatalog.pig.HCatLoader();      

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.

work_DM_FR_univ_filter =
   FILTER work_DM_FR_univ BY (batch_id=='$batch_id');

work_DM_NL_univ_filter =
   FILTER work_DM_NL_univ BY (batch_id=='$batch_id');

work_DM_GE_univ_filter =
   FILTER work_DM_GE_univ BY (batch_id=='$batch_id');

work_DM_BE_univ_filter =
   FILTER work_DM_BE_univ BY (batch_id=='$batch_id');

work_DM_IT_univ_filter =
   FILTER work_DM_IT_univ BY (batch_id=='$batch_id');

work_DM_AT_univ_filter =
   FILTER work_DM_AT_univ BY (batch_id=='$batch_id');
   
work_DM_UK_univ_filter =
   FILTER work_DM_UK_univ BY (batch_id=='$batch_id');
   
work_DM_IR_univ_filter =
   FILTER work_DM_IR_univ BY (batch_id=='$batch_id');
   
work_DM_FR_univ_filter =
   FILTER work_DM_FR_univ BY (batch_id=='$batch_id');               

-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_FR_univ table

revenue_FR=
   FOREACH work_DM_FR_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_FR =
   GROUP revenue_FR BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_FR_univ =
   FOREACH group_revenue_cc_FR GENERATE
   FLATTEN(revenue_FR),
   SUM(revenue_FR.net_sales) AS tot_revenue,
   SUM(revenue_FR.big_deal) AS tot_big_deal,
   SUM(revenue_FR.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_FR_univ =
   FOREACH flatten_group_revenue_cc_work_DM_FR_univ GENERATE
   revenue_FR::fiscal_month as fiscal_month,
   revenue_FR::fiscal_year as fiscal_year,
   revenue_FR::customer_id as customer_id,
   revenue_FR::customer_nm as customer_nm,
   revenue_FR::associate_id as associate_id,
   revenue_FR::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_FR::batch_id as batch_id,
   revenue_FR::country_cd as country_cd;
   
-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_NL_univ table
revenue_NL =
   FOREACH work_DM_NL_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_NL =
   GROUP revenue_NL BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_NL_univ =
   FOREACH group_revenue_cc_NL GENERATE
   FLATTEN(revenue_NL),
   SUM(revenue_NL.net_sales) AS tot_revenue,
   SUM(revenue_NL.big_deal) AS tot_big_deal,
   SUM(revenue_NL.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_NL_univ =
   FOREACH flatten_group_revenue_cc_work_DM_NL_univ GENERATE
   revenue_NL::fiscal_month as fiscal_month,
   revenue_NL::fiscal_year as fiscal_year,
   revenue_NL::customer_id as customer_id,
   revenue_NL::customer_nm as customer_nm,
   revenue_NL::associate_id as associate_id,
   revenue_NL::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_NL::batch_id as batch_id,
   revenue_NL::country_cd as country_cd;
   
-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_GE_univ table
revenue_GE =
   FOREACH work_DM_GE_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_GE =
   GROUP revenue_GE BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_GE_univ =
   FOREACH group_revenue_cc_GE GENERATE
   FLATTEN(revenue_GE),
   SUM(revenue_GE.net_sales) AS tot_revenue,
   SUM(revenue_GE.big_deal) AS tot_big_deal,
   SUM(revenue_GE.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_GE_univ =
   FOREACH flatten_group_revenue_cc_work_DM_GE_univ GENERATE
   revenue_GE::fiscal_month as fiscal_month,
   revenue_GE::fiscal_year as fiscal_year,
   revenue_GE::customer_id as customer_id,
   revenue_GE::customer_nm as customer_nm,
   revenue_GE::associate_id as associate_id,
   revenue_GE::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_GE::batch_id as batch_id,
   revenue_GE::country_cd as country_cd;
   
-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_BE_univ table
revenue_BE =
   FOREACH work_DM_BE_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_BE =
   GROUP revenue_BE BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_BE_univ =
   FOREACH group_revenue_cc_BE GENERATE
   FLATTEN(revenue_BE),
   SUM(revenue_BE.net_sales) AS tot_revenue,
   SUM(revenue_BE.big_deal) AS tot_big_deal,
   SUM(revenue_BE.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_BE_univ =
   FOREACH flatten_group_revenue_cc_work_DM_BE_univ GENERATE
   revenue_BE::fiscal_month as fiscal_month,
   revenue_BE::fiscal_year as fiscal_year,
   revenue_BE::customer_id as customer_id,
   revenue_BE::customer_nm as customer_nm,
   revenue_BE::associate_id as associate_id,
   revenue_BE::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_BE::batch_id as batch_id,
   revenue_BE::country_cd as country_cd;
   
-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_IT_univ table
revenue_IT =
   FOREACH work_DM_IT_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_IT =
   GROUP revenue_IT BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_IT_univ =
   FOREACH group_revenue_cc_IT GENERATE
   FLATTEN(revenue_IT),
   SUM(revenue_IT.net_sales) AS tot_revenue,
   SUM(revenue_IT.big_deal) AS tot_big_deal,
   SUM(revenue_IT.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_IT_univ =
   FOREACH flatten_group_revenue_cc_work_DM_IT_univ GENERATE
   revenue_IT::fiscal_month as fiscal_month,
   revenue_IT::fiscal_year as fiscal_year,
   revenue_IT::customer_id as customer_id,
   revenue_IT::customer_nm as customer_nm,
   revenue_IT::associate_id as associate_id,
   revenue_IT::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_IT::batch_id as batch_id,
   revenue_IT::country_cd as country_cd;
   
-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_AT_univ table
revenue_AT =
   FOREACH work_DM_AT_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_AT =
   GROUP revenue_AT BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_AT_univ =
   FOREACH group_revenue_cc_AT GENERATE
   FLATTEN(revenue_AT),
   SUM(revenue_AT.net_sales) AS tot_revenue,
   SUM(revenue_AT.big_deal) AS tot_big_deal,
   SUM(revenue_AT.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_AT_univ =
   FOREACH flatten_group_revenue_cc_work_DM_AT_univ GENERATE
   revenue_AT::fiscal_month as fiscal_month,
   revenue_AT::fiscal_year as fiscal_year,
   revenue_AT::customer_id as customer_id,
   revenue_AT::customer_nm as customer_nm,
   revenue_AT::associate_id as associate_id,
   revenue_AT::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_AT::batch_id as batch_id,
   revenue_AT::country_cd as country_cd;
   
-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_UK_univ table
revenue_UK =
   FOREACH work_DM_UK_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_UK =
   GROUP revenue_UK BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_UK_univ =
   FOREACH group_revenue_cc_UK GENERATE
   FLATTEN(revenue_UK),
   SUM(revenue_UK.net_sales) AS tot_revenue,
   SUM(revenue_UK.big_deal) AS tot_big_deal,
   SUM(revenue_UK.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_UK_univ =
   FOREACH flatten_group_revenue_cc_work_DM_UK_univ GENERATE
   revenue_UK::fiscal_month as fiscal_month,
   revenue_UK::fiscal_year as fiscal_year,
   revenue_UK::customer_id as customer_id,
   revenue_UK::customer_nm as customer_nm,
   revenue_UK::associate_id as associate_id,
   revenue_UK::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_UK::batch_id as batch_id,
   revenue_UK::country_cd as country_cd;
   
-- GROUPING TO TAKE SUM OF NET SALES :: work_DM_IR_univ table
revenue_IR =
   FOREACH work_DM_IR_univ_filter GENERATE
   fiscal_month,
   fiscal_year,
   customer_id,
   customer_nm,
   associate_id,
   associate_full_name,
   (double)net_sales,
   (int)big_deal,
   (int)cogs,
   batch_id,
   country_cd;
   
group_revenue_cc_IR =
   GROUP revenue_IR BY (associate_id, customer_id, fiscal_month, fiscal_year);

-- Flattening the bag to tuples
flatten_group_revenue_cc_work_DM_IR_univ =
   FOREACH group_revenue_cc_IR GENERATE
   FLATTEN(revenue_IR),
   SUM(revenue_IR.net_sales) AS tot_revenue,
   SUM(revenue_IR.big_deal) AS tot_big_deal,
   SUM(revenue_IR.cogs) AS tot_cogs;

--Generating only required fiels

final_work_DM_IR_univ =
   FOREACH flatten_group_revenue_cc_work_DM_IR_univ GENERATE
   revenue_IR::fiscal_month as fiscal_month,
   revenue_IR::fiscal_year as fiscal_year,
   revenue_IR::customer_id as customer_id,
   revenue_IR::customer_nm as customer_nm,
   revenue_IR::associate_id as associate_id,
   revenue_IR::associate_full_name as associate_full_name,
   (double)tot_revenue as tot_revenue,
   (double)tot_big_deal as big_deal,
   (double)tot_cogs as cogs,
   revenue_IR::batch_id as batch_id,
   revenue_IR::country_cd as country_cd;
   
-- Taking Unique records
   distinct_final_work_DM_FR_univ = DISTINCT final_work_DM_FR_univ;
   distinct_final_work_DM_NL_univ = DISTINCT final_work_DM_NL_univ;
   distinct_final_work_DM_GE_univ = DISTINCT final_work_DM_GE_univ;
   distinct_final_work_DM_BE_univ = DISTINCT final_work_DM_BE_univ;
   distinct_final_work_DM_IT_univ = DISTINCT final_work_DM_IT_univ;
   distinct_final_work_DM_AT_univ = DISTINCT final_work_DM_AT_univ;
   distinct_final_work_DM_UK_univ = DISTINCT final_work_DM_UK_univ;
   distinct_final_work_DM_IR_univ = DISTINCT final_work_DM_IR_univ;

--##############################################################################
--#                                   Store                                    #
--##############################################################################
  
STORE distinct_final_work_DM_FR_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_fr'
  USING parquet.pig.ParquetStorer;
STORE distinct_final_work_DM_NL_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_nl'
  USING parquet.pig.ParquetStorer;
STORE distinct_final_work_DM_GE_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_ge'
  USING parquet.pig.ParquetStorer;
STORE distinct_final_work_DM_BE_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_be'
  USING parquet.pig.ParquetStorer;
STORE distinct_final_work_DM_IT_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_it'
  USING parquet.pig.ParquetStorer;
STORE distinct_final_work_DM_AT_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_at'
  USING parquet.pig.ParquetStorer;
STORE distinct_final_work_DM_UK_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_uk'
  USING parquet.pig.ParquetStorer;
STORE distinct_final_work_DM_IR_univ INTO '$GOLD_HDFS/$GOLD_CDMPRDDTA_DM/batch_id=$batch_id/country_cd=$country_code_ir'
  USING parquet.pig.ParquetStorer;
  

--##############################################################################
--#                                   End                                    #
--##############################################################################
  