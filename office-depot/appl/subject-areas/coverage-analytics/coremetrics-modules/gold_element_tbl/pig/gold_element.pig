--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_element                      								   #
--# File                                                                       #
--#     : gold_element.pig                                                     #
--# Description                                                                #
--#     : To load data into gold_element                                       #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

 SET job.name '$GOLD_ELEMENT_SSH_ACTION';
 SET default_parallel 5;
-- Loading data from incoming_clickstream.incoming_element table.
gold_element = 
   LOAD '$DB_INCOMING.$INCOMING_ELEMENT' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_element_filter = 
   FILTER gold_element BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
   
    
-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
 FOREACH gold_element_filter GENERATE
 (chararray)session_id AS session_id,
 (chararray)cookie_id AS cookie_id,
 timestamp AS timestamp,
 element_name AS element_name, 
 element_category AS element_category,
 site_id AS site_id,
 product_page_sku AS product_page_sku,
 endecca_refinement_category AS endecca_refinement_category,
 endecca_refinement AS endecca_refinement,
 error_messaging_product AS error_messaging_product,
 endecca_refinement_product_category AS endecca_refinement_product_category,
 csl_add_to_list_from_location AS csl_add_to_list_from_location,
 csl_add_to_list_type AS csl_add_to_list_type,
 csl_create_list_from AS csl_create_list_from,
 csl_manage_list_link_from AS csl_manage_list_link_from,
 csl_paginate_depth_lb_pg_num AS csl_paginate_depth_lb_pg_num,
 csl_paginate_from AS csl_paginate_from,
 csl_set_default_list_from AS csl_set_default_list_from,
 csl_sort_list_by AS csl_sort_list_by,
 csl_sort_list_from AS csl_sort_list_from,
 grand_total AS grand_total,
 subscription_sku AS subscription_sku,
 monetate_element AS monetate_element,
 test_exposure AS test_exposure,
 local_browse_search_term AS local_browse_search_term,
 tab_name AS tab_name,
 order_hist_page_srch_item AS order_hist_page_srch_item,
 order_hist_page_srch_ord_type AS order_hist_page_srch_ord_type,
 order_hist_pg_srch_ord_status AS order_hist_pg_srch_ord_status,
 order_hist_pg_prod_grp_filter AS order_hist_pg_prod_grp_filter,
 order_hist_pg_prod_cat_filter AS order_hist_pg_prod_cat_filter,
 order_hist_pg_sub_cat_filter AS order_hist_pg_sub_cat_filter,
 video_status AS video_status,
 video_time_stamp AS video_time_stamp,
 video_length AS video_length,
 predictive_test_painted_result AS predictive_test_painted_result,
 predictive_test_search_term AS predictive_test_search_term,
 element_attribute_32 AS element_attribute_32,
 element_attribute_33 AS element_attribute_33,
 element_attribute_34 AS element_attribute_34,
 element_attribute_35 AS element_attribute_35,
 element_attribute_36 AS element_attribute_36,
 element_attribute_37 AS element_attribute_37,
 element_attribute_38 AS element_attribute_38,
 element_attribute_39 AS element_attribute_39,
 element_attribute_40 AS element_attribute_40,
 element_attribute_41 AS element_attribute_41,
 element_attribute_42 AS element_attribute_42,
 element_attribute_43 AS element_attribute_43,
 element_attribute_44 AS element_attribute_44,
 element_attribute_45 AS element_attribute_45,
 element_attribute_46 AS element_attribute_46,
 element_attribute_47 AS element_attribute_47,
 element_attribute_48 AS element_attribute_48,
 element_attribute_49 AS element_attribute_49,
 element_attribute_50 AS element_attribute_50,
 batch_id AS batch_id;   
   
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_element table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_ELEMENT/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_ELEMENT'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
