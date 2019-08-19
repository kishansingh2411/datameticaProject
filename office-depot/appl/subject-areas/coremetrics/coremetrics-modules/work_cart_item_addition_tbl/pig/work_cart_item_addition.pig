--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cart_item_addition      								   		   #
--# File                                                                       #
--#     : work_cart_item_addition.pig                                   	   #
--# Description                                                                #
--#     : To load data into work_cart_item_addition table.              	   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$WORK_CART_ITEM_ADDITION_SSH_ACTION';

--Loading data from gold_clickstream.gold_cart_item_addition table.
work_cart_item_addition = 
   LOAD '$DB_GOLD.$GOLD_CART_ITEM_ADDITION'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and not null session_id.
work_cart_item_addition_filter = 
   FILTER work_cart_item_addition BY (batch_id=='$batch_id' AND session_id IS NOT NULL);

-- Selecting required fields from table.
-- If value of country will be null it will be replaced with 'DEFAULT'.   
work_cart_item_addition_generate = 
   FOREACH work_cart_item_addition_filter GENERATE
   session_id AS session_id,visitor_segment,
   (country_description IS NULL OR country_description=='' OR country_description==' ' ? '$DEF_VALUE':country_description) AS country_description;

-- Selecting distinct records, as we need total cart addition and
-- a session_id will repeat because customer will add more than 1 item in cart.   
work_cart_item_addition_distinct = 
   DISTINCT work_cart_item_addition_generate;

-- Grouping cart addition records by country_description.
work_cart_item_addition_group = 
   GROUP work_cart_item_addition_distinct BY (country_description,visitor_segment);

-- Generating total cart addition records by country_description.
generate_records = 
		FOREACH work_cart_item_addition_group GENERATE 
		FLATTEN(group) as (country_description,visitor_segment),
		COUNT_STAR(work_cart_item_addition_distinct.session_id) AS total_cart_started,
		'$batch_id' AS batch_id;
		
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing data in work_clickstream.work_cart_item_adition table.
STORE generate_records 
   INTO '$DB_WORK.$WORK_CART_ITEM_ADDITION' 
   USING org.apache.hive.hcatalog.pig.HCatStorer();

--##############################################################################
--#                                    End                                     #
--##############################################################################
