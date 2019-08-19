--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : outgoing_clickstream_view_daily      								   #
--# File                                                                       #
--#     : outgoing_clickstream_view_daily.pig                                  #
--# Description                                                                #
--#     : To load data into outgoing layer                                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$OUTGOING_SCHEMA_SSH_ACTION';
SET default_parallel 2;
-- Loading data from work_clickstream.work_product_view table.
outgoing_product_view = 
   LOAD '$DB_WORK.$WORK_PRODUCT_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_clickstream.work_cart_item_addition table.
outgoing_cart_item_addition = 
   LOAD '$DB_WORK.$WORK_CART_ITEM_ADDITION'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_clickstream.work_cart_item_abandonment table.   
outgoing_cart_item_abandonment = 
   LOAD '$DB_WORK.$WORK_CART_ITEM_ABANDONMENT'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_clickstream.work_page_view table.   
outgoing_page_view = 
   LOAD '$DB_WORK.$WORK_PAGE_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from work_clickstream.work_order table.
outgoing_order = 
   LOAD '$DB_WORK.$WORK_ORDER'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
outgoing_product_view_filter = 
   FILTER outgoing_product_view BY (batch_id == '$batch_id');
   
outgoing_cart_item_addition_filter = 
   FILTER outgoing_cart_item_addition BY (batch_id == '$batch_id');
   
outgoing_cart_item_abandonment_filter = 
   FILTER outgoing_cart_item_abandonment BY (batch_id == '$batch_id');
   
outgoing_page_view_filter = 
   FILTER outgoing_page_view BY (batch_id == '$batch_id');
   
 outgoing_order_filter =
   FILTER outgoing_order BY (batch_id == '$batch_id');           

-- Applying Full Outer Join on work_page_view_filter and work_cart_item_addition table.
outgoing_page_addition_join = 
   JOIN outgoing_page_view_filter BY (country_description,visitor_segment) FULL OUTER, 
   outgoing_cart_item_addition_filter BY (country_description,visitor_segment);

-- Applying Full Outer Join on outgoing_page_addition_join alias and work_cart_item_abandonment table.
outgoing_page_addition_abandonment_join = 
   JOIN outgoing_page_addition_join BY (outgoing_page_view_filter::country_description,outgoing_cart_item_addition_filter::visitor_segment) FULL OUTER, 
   outgoing_cart_item_abandonment_filter BY (country_description,visitor_segment);

-- Applying Full Outer Join on outgoing_page_addition_abandonment_join alias and work_product_view table.   
outgoing_page_addition_abandonment_product_join = 
   JOIN outgoing_page_addition_abandonment_join BY (outgoing_page_addition_join::outgoing_page_view_filter::country_description,outgoing_page_addition_join::outgoing_page_view_filter::visitor_segment) FULL OUTER, 
   outgoing_product_view_filter BY (country_description,visitor_segment);

-- Applying Full Outer Join on outgoing_page_addition_abandonment_product_join alias and work_order table.   
outgoing_page_addition_abandonment_product_order_join = 
   JOIN outgoing_page_addition_abandonment_product_join BY (outgoing_page_addition_abandonment_join::outgoing_page_addition_join::outgoing_page_view_filter::country_description,outgoing_page_addition_abandonment_join::outgoing_page_addition_join::outgoing_page_view_filter::visitor_segment) FULL OUTER, 
   outgoing_order_filter BY (country_description,visitor_segment); 


-- Generating final records to push to hbase.
generate_records = 
   FOREACH outgoing_page_addition_abandonment_product_order_join GENERATE
    $4 AS load_date,
   $0 AS country_description,
   $1 AS visitor_segment,
   ($20 IS NULL?0:$20) AS total_orders,
   ($3 IS NULL?0:$3) AS total_visits,
   ($2 IS NULL?0:$2)  AS total_page_views,
   ($21 IS NULL?0:$21) AS total_revenue_from_new_customer,
   ($22 IS NULL?0:$22) AS total_revenue_from_existing_customer,
   ($8 IS NULL?0:$8) AS total_cart_started,
   ($12 IS NULL?0:$12) AS total_cart_abandonment,
   ($16 IS NULL?0:$16) AS total_view_products,
   ($23 IS NULL?0:$23) AS total_revenue_from_anonymous_customer;

-- Filtering records to make sure valid data is pushed to hbase. 
generate_records_filter = 
   FILTER generate_records BY (country_description IS NOT NULL);   
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into /office_depot/clickstream/outgoing/daily/batch_id/ directory.
STORE generate_records_filter 
   INTO '$OUTPUT_DIRECTORY/$record_type/$batch_id/' 
   USING PigStorage(',');
   
--##############################################################################
--#                                    End                                     #
--##############################################################################
