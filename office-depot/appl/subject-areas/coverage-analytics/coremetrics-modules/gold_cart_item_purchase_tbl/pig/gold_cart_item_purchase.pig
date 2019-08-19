--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_cart_item_purchase            								   #
--# File                                                                       #
--#     : gold_cart_item_purchase.pig                                          #
--# Description                                                                #
--#     : To load data into gold_cart_item_purchase                            #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_CART_ITEM_PURCHASE_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_cart_item_purchase table.
gold_cart_item_purchase = 
   LOAD '$DB_INCOMING.$INCOMING_CART_ITEM_PURCHASE' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_cart_item_purchase_filter = 
   FILTER gold_cart_item_purchase BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');

-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
FOREACH gold_cart_item_purchase_filter 
GENERATE
(chararray)session_id AS session_id,
(chararray)cookie_id AS cookie_id,
timestampAS timestamp, 
product_id  AS product_id,
product_name AS product_name,
product_category AS product_category,
product_category_id AS product_category_id,
product_category_top AS product_category_top,
product_category_bottom AS product_category_bottom,
base_price AS base_price,
(chararray)quantity AS quantity,
order_id AS order_id,
site_id AS site_id,
delivery_mode AS delivery_mode,
avg_rating AS avg_rating,
total_review_count AS total_review_count,
brand_code AS brand_code,
product_avaliability AS product_avaliability,
promo_type AS promo_type,
shipping_description AS shipping_description,
purchase_attribute_8 AS purchase_attribute_8,
purchase_attribute_9 AS purchase_attribute_9,
b2b_b2c AS b2b_b2c,
purchase_attribute_11 AS purchase_attribute_11,
effort_code AS effort_code,
discount_total AS discount_total,
purchase_attribute_14 AS purchase_attribute_14,
atc_source AS atc_source,
list_source AS list_source,
ssc_list_grade AS ssc_list_grade,
atc_location AS atc_location,
atc_referral_pid AS atc_referral_pid,
atc_referral_url AS atc_referral_url,
shop_21 AS shop_21,
style_id AS style_id,
style_description AS style_description,
purchase_attribute_24 AS purchase_attribute_24,
purchase_attribute_25 AS purchase_attribute_25,
purchase_attribute_26 As purchase_attribute_26,
purchase_attribute_27 AS purchase_attribute_27,
purchase_attribute_28 AS purchase_attribute_28,
purchase_attribute_29 AS purchase_attribute_29,
purchase_attribute_30 As purchase_attribute_30,
purchase_attribute_31 AS purchase_attribute_31,
purchase_attribute_32 AS purchase_attribute_32,
purchase_attribute_33 As purchase_attribute_33,
purchase_attribute_34 AS purchase_attribute_34,
purchase_attribute_35 AS purchase_attribute_35,
purchase_attribute_36 AS purchase_attribute_36,
purchase_attribute_37 As purchase_attribute_37,
purchase_attribute_38 AS purchase_attribute_38,
purchase_attribute_39 AS purchase_attribute_39,
purchase_attribute_40 AS purchase_attribute_40,
purchase_attribute_41 AS purchase_attribute_41,
purchase_attribute_42 AS purchase_attribute_42,
purchase_attribute_43 AS purchase_attribute_43,
purchase_attribute_44 AS purchase_attribute_44,
purchase_attribute_45 As purchase_attribute_45,
purchase_attribute_46 As purchase_attribute_46,
purchase_attribute_47 AS purchase_attribute_47,
purchase_attribute_48 AS purchase_attribute_48,
purchase_attribute_49 AS purchase_attribute_49,
purchase_attribute_50 AS purchase_attribute_50,
batch_id AS batch_id;   


--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_cart_item_purchase table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_CART_ITEM_PURCHASE/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_CART_ITEM_PURCHASE'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
