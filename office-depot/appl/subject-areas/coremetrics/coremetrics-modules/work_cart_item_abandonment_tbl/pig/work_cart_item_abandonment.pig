--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cart_item_abandonment      								   	   #
--# File                                                                       #
--#     : work_cart_item_abandonment.pig                                   	   #
--# Description                                                                #
--#     : To load data into work_cart_item_abandonment table.              	   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$WORK_CART_ITEM_ABANDONMENT_SSH_ACTION';

-- Loading data from gold_clickstream.gold_cart_item_abandonment table.
work_cart_item_abandonment = 
   LOAD '$DB_GOLD.$GOLD_CART_ITEM_ABANDONMENT'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and not null session_id.
work_cart_item_abandonment_filter = 
   FILTER work_cart_item_abandonment BY (batch_id=='$batch_id' AND session_id IS NOT NULL);

-- Selecting required fields from table.
-- If value of country will be null it will be replaced with 'DEFAULT'.   
work_cart_item_abandonment_generate = 
   FOREACH work_cart_item_abandonment_filter GENERATE
   session_id AS session_id,visitor_segment,
   (country_description IS NULL OR country_description=='' OR country_description==' ' ? '$DEF_VALUE':country_description) AS country_description;

-- Selecting distinct records, as we need total cart abandonments and
-- a session_id may repeat.   
work_cart_item_abandonment_distinct = 
   DISTINCT work_cart_item_abandonment_generate;

-- Grouping cart abandonment records by country_description.
work_cart_item_abandonment_group = 
   GROUP work_cart_item_abandonment_distinct BY (country_description,visitor_segment);

-- generating total cart abandonment records by country_description.
generate_records = 
    FOREACH work_cart_item_abandonment_group GENERATE 
    FLATTEN(group) as (country_description,visitor_segment),
    COUNT_STAR(work_cart_item_abandonment_distinct.session_id) AS total_cart_abandonment,
    '$batch_id' AS batch_id;
   
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing data in work_clickstream.work_cart_item_abandonment table.
STORE generate_records 
   INTO '$DB_WORK.$WORK_CART_ITEM_ABANDONMENT' 
   USING org.apache.hive.hcatalog.pig.HCatStorer();

--##############################################################################
--#                                    End                                     #
--##############################################################################
