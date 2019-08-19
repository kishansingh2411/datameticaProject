--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_conversion_event             								   #
--# File                                                                       #
--#     : gold_conversion_event.pig                                            #
--# Description                                                                #
--#     : To load data into gold_conversion_event                              #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_CONVERSION_EVENT_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_conversion_event table.
gold_conversion_event = 
   LOAD '$DB_INCOMING.$INCOMING_CONVERSION_EVENT' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_conversion_event_filter = 
   FILTER gold_conversion_event BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
   
    
-- Generating required fields from incoming_product_view and work_geography_country_cookie table.

generate_records = 
FOREACH gold_conversion_event_filter GENERATE
(chararray)session_id AS session_id,
(chararray)cookie_id AS cookie_id,
timestamp AS time_stamp,
event_name AS event_name,
event_category AS event_category, 
(chararray)event_action_type AS event_action_type,
(chararray)event_points AS event_points,
site_id AS site_id,
ce_ex1_smart_cart_user_id AS ce_ex1_smart_cart_user_id,
ce_ex2_smart_cart_account AS ce_ex2_smart_cart_account,
ce_ex3_smartcartoriginal_sku AS ce_ex3_smartcartoriginal_sku,
ce_ex4_smart_cart_original_q AS ce_ex4_smart_cart_original_q,
ce_ex5_smart_cart_original_p AS ce_ex5_smart_cart_original_p,
ce_ex6_smartcartpurchased_sku AS ce_ex6_smartcartpurchased_sku,
ce_ex7_smartcartpurchased_q AS ce_ex7_smartcartpurchased_q,
ce_ex8_smartcartpurchased_p AS ce_ex8_smartcartpurchased_p,
ce_ex9_smart_cart_savings AS ce_ex9_smart_cart_savings,
ce_ex10_signup_city AS ce_ex10_signup_city,
ce_ex11_signup_state AS ce_ex11_signup_state,
ce_ex12_signup_number_of_emp AS ce_ex12_signup_number_of_emp,
ce_ex13_signup_bus_industry AS ce_ex13_signup_bus_industry,
ce_ex14_signup_title AS ce_ex14_signup_title,
subscription_sku AS subscription_sku,
conversion_event_attribute_16 AS conversion_event_attribute_16,
conversion_event_attribute_17 AS conversion_event_attribute_17,
conversion_event_attribute_18 AS conversion_event_attribute_18,
conversion_event_attribute_19 AS conversion_event_attribute_19,
conversion_event_attribute_20 AS conversion_event_attribute_20,
conversion_event_attribute_21 AS conversion_event_attribute_21,
conversion_event_attribute_22 AS conversion_event_attribute_22,
conversion_event_attribute_23 AS conversion_event_attribute_23,
conversion_event_attribute_24 AS conversion_event_attribute_24,
conversion_event_attribute_25 AS conversion_event_attribute_25,
conversion_event_attribute_26 As conversion_event_attribute_26,
conversion_event_attribute_27 AS conversion_event_attribute_27,
conversion_event_attribute_28 AS conversion_event_attribute_28,
conversion_event_attribute_29 AS conversion_event_attribute_29,
conversion_event_attribute_30 AS conversion_event_attribute_30,
conversion_event_attribute_31 AS conversion_event_attribute_31,
conversion_event_attribute_32 AS conversion_event_attribute_32,
conversion_event_attribute_33 AS conversion_event_attribute_33,
conversion_event_attribute_34 AS conversion_event_attribute_34,
conversion_event_attribute_35 AS conversion_event_attribute_35,
conversion_event_attribute_36 AS conversion_event_attribute_36,
conversion_event_attribute_37 AS conversion_event_attribute_37,
conversion_event_attribute_38 AS conversion_event_attribute_38,
conversion_event_attribute_39 AS conversion_event_attribute_39,
conversion_event_attribute_40 AS conversion_event_attribute_40,
conversion_event_attribute_41 AS conversion_event_attribute_41,
conversion_event_attribute_42 AS conversion_event_attribute_42,
conversion_event_attribute_43 AS conversion_event_attribute_43,
conversion_event_attribute_44 AS conversion_event_attribute_44,
conversion_event_attribute_45 AS conversion_event_attribute_45,
conversion_event_attribute_46 AS conversion_event_attribute_46,
conversion_event_attribute_47 AS conversion_event_attribute_47,
conversion_event_attribute_48 AS conversion_event_attribute_48,
conversion_event_attribute_49 AS conversion_event_attribute_49,
conversion_event_attribute_50 AS conversion_event_attribute_50,
batch_id AS batch_id;    
   

--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_conversion_event table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_CONVERSION_EVENT/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_CONVERSION_EVENT'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
