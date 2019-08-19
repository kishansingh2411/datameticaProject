--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_product_view      								   		       #
--# File                                                                       #
--#     : work_product_view.pig                                   	           #
--# Description                                                                #
--#     : To load data into work_product_view table.              	           #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$WORK_PRODUCT_VIEW_SSH_ACTION';

--Loading data from gold_clickstream.gold_product_view table.
work_product_view = 
   LOAD '$DB_GOLD.$GOLD_PRODUCT_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and not null session_id.
work_product_view_filter = 
   FILTER work_product_view BY (batch_id=='$batch_id' AND session_id IS NOT NULL);

-- Selecting required fields from table.
-- If value of country will be null it will be replaced with 'DEFAULT'.   
work_product_view_generate = 
   FOREACH work_product_view_filter GENERATE
   session_id AS session_id,
   visitor_segment AS visitor_segment,
   (country_description IS NULL OR country_description=='' OR country_description==' ' ? '$DEF_VALUE':country_description) AS country_description;

-- Selecting distinct records, as we need total customers who viewed product and
-- a session_id will repeat because a customer will view more than one product.   
work_product_view_distinct = 
   DISTINCT work_product_view_generate;

-- Grouping product view records by country.
work_product_view_group = 
   GROUP work_product_view_distinct BY (country_description,visitor_segment);

-- Generating total product viewed records by country.
generate_records = 
   FOREACH work_product_view_group GENERATE 
   FLATTEN(group) as (country_description,visitor_segment),
   COUNT_STAR(work_product_view_distinct.session_id) AS total_customers_who_viewed_product,
   '$batch_id' AS batch_id;
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing data in work_clickstream.work_product_view table.
STORE generate_records 
   INTO '$DB_WORK.$WORK_PRODUCT_VIEW' 
   USING org.apache.hive.hcatalog.pig.HCatStorer();

--##############################################################################
--#                                    End                                     #
--##############################################################################
