--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_cart_item_abandonment      								   	   #
--# File                                                                       #
--#     : gold_cart_item_abandonment.pig                                   	   #
--# Description                                                                #
--#     : To load data into gold_cart_item_abandonment table.              	   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_CART_ITEM_ABANDONMENT_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_cart_item_abandonment table
gold_cart_item_abandonment = 
   LOAD '$DB_INCOMING.$INCOMING_CART_ITEM_ABANDONMENT'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_clickstream.work_geography_country_cookie table
work_geography_country_cookie =
   LOAD '$DB_WORK.$WORK_GEOGRAPHY_COUNTRY_COOKIE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and header.
gold_cart_item_abandonment_filter = 
   FILTER gold_cart_item_abandonment BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');

-- Filtering data for current batch_id.   
work_geography_country_cookie_filter = 
   FILTER work_geography_country_cookie BY (batch_id=='$batch_id');

-- Applying Left Outer Join on incoming_cart_item_abandonment and work_geography_country_cookie table.   
gold_cart_item_abandonment_geography_join = 
   JOIN gold_cart_item_abandonment_filter BY session_id LEFT OUTER, 
   work_geography_country_cookie_filter BY session_id;

-- Generating required fields from incoming_cart_item_abandonment and work_geography_country_cookie table.
generate_records = 
    FOREACH gold_cart_item_abandonment_geography_join GENERATE
   (chararray)gold_cart_item_abandonment_filter::session_id AS session_id,
    (chararray)gold_cart_item_abandonment_filter::cookie_id AS cookie_id,
   gold_cart_item_abandonment_filter::timestamp AS timestamp,
   gold_cart_item_abandonment_filter::product_id AS product_id,
   gold_cart_item_abandonment_filter::product_name AS product_name,
   gold_cart_item_abandonment_filter::product_category AS product_category,
   gold_cart_item_abandonment_filter::product_category_id AS product_category_id,
   gold_cart_item_abandonment_filter::product_category_top AS product_category_top,
   gold_cart_item_abandonment_filter::product_category_bottom AS product_category_bottom,
   gold_cart_item_abandonment_filter::base_price AS base_price,
    (chararray)	gold_cart_item_abandonment_filter::quantity AS quantity,
   gold_cart_item_abandonment_filter::order_id AS order_id,
   gold_cart_item_abandonment_filter::site_id AS site_id,
   gold_cart_item_abandonment_filter::delivery_mode AS delivery_mode,
   gold_cart_item_abandonment_filter::avg_rating AS avg_rating,
   gold_cart_item_abandonment_filter::total_review_count AS total_review_count,
   gold_cart_item_abandonment_filter::brand_code AS brand_code,
   gold_cart_item_abandonment_filter::product_avaliability AS product_avaliability,
   gold_cart_item_abandonment_filter::promo_type AS promo_type,
   gold_cart_item_abandonment_filter::shipping_description AS shipping_description,
   gold_cart_item_abandonment_filter::abandonment_attribute_8 AS abandonment_attribute_8,
   gold_cart_item_abandonment_filter::abandonment_attribute_9 AS abandonment_attribute_9,
   gold_cart_item_abandonment_filter::b2b_b2c AS b2b_b2c,
   gold_cart_item_abandonment_filter::abandonment_attribute_11 AS abandonment_attribute_11,
   gold_cart_item_abandonment_filter::effort_code AS effort_code,
   gold_cart_item_abandonment_filter::discount_total AS discount_total,
   gold_cart_item_abandonment_filter::abandonment_attribute_14 AS abandonment_attribute_14,
   gold_cart_item_abandonment_filter::atc_source AS atc_source,
   gold_cart_item_abandonment_filter::list_source AS list_source,
   gold_cart_item_abandonment_filter::ssc_list_grade AS ssc_list_grade,
   gold_cart_item_abandonment_filter::atc_location AS atc_location,
   gold_cart_item_abandonment_filter::atc_referral_pid AS atc_referral_pid,
   gold_cart_item_abandonment_filter::atc_referral_url AS atc_referral_url,
   gold_cart_item_abandonment_filter::shop_21 AS shop_21,
   gold_cart_item_abandonment_filter::style_id AS style_id,
   gold_cart_item_abandonment_filter::style_description AS style_description,
   gold_cart_item_abandonment_filter::abandonment_attribute_24 AS abandonment_attribute_24,
   gold_cart_item_abandonment_filter::abandonment_attribute_25 AS abandonment_attribute_25,
   gold_cart_item_abandonment_filter::abandonment_attribute_26 AS abandonment_attribute_26,
   gold_cart_item_abandonment_filter::abandonment_attribute_27 AS abandonment_attribute_27,
   gold_cart_item_abandonment_filter::abandonment_attribute_28 AS abandonment_attribute_28,
   gold_cart_item_abandonment_filter::abandonment_attribute_29 AS abandonment_attribute_29,
   gold_cart_item_abandonment_filter::abandonment_attribute_30 AS abandonment_attribute_30,
   gold_cart_item_abandonment_filter::abandonment_attribute_31 AS abandonment_attribute_31,
   gold_cart_item_abandonment_filter::abandonment_attribute_32 AS abandonment_attribute_32,
   gold_cart_item_abandonment_filter::abandonment_attribute_33 AS abandonment_attribute_33,
   gold_cart_item_abandonment_filter::abandonment_attribute_34 AS abandonment_attribute_34,
   gold_cart_item_abandonment_filter::abandonment_attribute_35 AS abandonment_attribute_35,
   gold_cart_item_abandonment_filter::abandonment_attribute_36 AS abandonment_attribute_36,
   gold_cart_item_abandonment_filter::abandonment_attribute_37 AS abandonment_attribute_37,
   gold_cart_item_abandonment_filter::abandonment_attribute_38 AS abandonment_attribute_38,
   gold_cart_item_abandonment_filter::abandonment_attribute_39 AS abandonment_attribute_39,
   gold_cart_item_abandonment_filter::abandonment_attribute_40 AS abandonment_attribute_40,
   gold_cart_item_abandonment_filter::abandonment_attribute_41 AS abandonment_attribute_41,
   gold_cart_item_abandonment_filter::abandonment_attribute_42 AS abandonment_attribute_42,
   gold_cart_item_abandonment_filter::abandonment_attribute_43 AS abandonment_attribute_43,
   gold_cart_item_abandonment_filter::abandonment_attribute_44 AS abandonment_attribute_44,
   gold_cart_item_abandonment_filter::abandonment_attribute_45 AS abandonment_attribute_45,
   gold_cart_item_abandonment_filter::abandonment_attribute_46 AS abandonment_attribute_46,
   gold_cart_item_abandonment_filter::abandonment_attribute_47 AS abandonment_attribute_47,
   gold_cart_item_abandonment_filter::abandonment_attribute_48 AS abandonment_attribute_48,
   gold_cart_item_abandonment_filter::abandonment_attribute_49 AS abandonment_attribute_49,
   gold_cart_item_abandonment_filter::abandonment_attribute_50 AS abandonment_attribute_50,
   work_geography_country_cookie_filter::ip_address AS ip_address,
   work_geography_country_cookie_filter::city AS city,
   work_geography_country_cookie_filter::state AS state,
   work_geography_country_cookie_filter::country AS country,
   work_geography_country_cookie_filter::dma AS dma,
   work_geography_country_cookie_filter::second_level_domain AS second_level_domain,
   work_geography_country_cookie_filter::client_name AS client_name,
   work_geography_country_cookie_filter::country_code AS country_code,
   work_geography_country_cookie_filter::language AS language,
   work_geography_country_cookie_filter::country_description AS country_description,
   work_geography_country_cookie_filter::visitor_segment AS visitor_segment,
   gold_cart_item_abandonment_filter::batch_id AS batch_id;

--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_cart_item_abandonment table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_CART_ITEM_ABANDONMENT/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_CART_ITEM_ABANDONMENT'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
