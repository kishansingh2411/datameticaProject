--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_order                                                           #
--# File                                                                       #
--#     : gold_order.pig                                                       #
--# Description                                                                #
--#     : To load data into gold_order table.                                  #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                                     #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_ORDER_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_order table
gold_order =
   LOAD '$DB_INCOMING.$INCOMING_ORDER'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_clickstream.work_geography_country_cookie table.
work_geography_country_cookie =
   LOAD '$DB_WORK.$WORK_GEOGRAPHY_COUNTRY_COOKIE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and header.
gold_order_filter =
   FILTER gold_order BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');

-- Filtering data for current batch_id.
work_geography_country_cookie_filter =
   FILTER work_geography_country_cookie BY (batch_id=='$batch_id');

-- Applying Left Outer Join on incoming_order and work_geography_country_cookie table.
gold_order_geography_join =
   JOIN gold_order_filter BY session_id LEFT OUTER,
   work_geography_country_cookie_filter BY session_id;

-- Generating required fields from incoming_order and work_geography_country_cookie table.
generate_records =
    FOREACH gold_order_geography_join GENERATE
   (chararray)gold_order_filter::session_id AS session_id,
   (chararray)gold_order_filter::cookie_id AS cookie_id,
   gold_order_filter::timestamp AS timestamp,
   gold_order_filter::order_id AS order_id,
   gold_order_filter::registration_id AS registration_id,
   gold_order_filter::order_total AS order_total,
   gold_order_filter::shipping AS shipping,
   gold_order_filter::site_id AS site_id,
   gold_order_filter::delivery_mode AS delivery_mode,
   gold_order_filter::account_type AS account_type,
   gold_order_filter::customer_type_b2b_or_b2c AS customer_type_b2b_or_b2c,
   gold_order_filter::customer_type AS customer_type,
   gold_order_filter::order_attribute_5 AS order_attribute_5,
   gold_order_filter::order_attribute_6 AS order_attribute_6,
   gold_order_filter::order_attribute_7 AS order_attribute_7,
   gold_order_filter::coupons_total AS coupons_total,
   gold_order_filter::association_discount AS association_discount,
   gold_order_filter::unknown_data_field AS unknown_data_field,
   gold_order_filter::tender_type_credit_card AS tender_type_credit_card,
   gold_order_filter::tender_type_paypal AS tender_type_paypal,
   gold_order_filter::tender_type_gift_card AS tender_type_gift_card,
   gold_order_filter::coupon_code AS coupon_code,
   gold_order_filter::loyaltyid AS loyaltyid,
   gold_order_filter::order_attribute_16 AS order_attribute_16,
   gold_order_filter::order_attribute_17 AS order_attribute_17,
   gold_order_filter::order_attribute_18 AS order_attribute_18,
   gold_order_filter::order_attribute_19 AS order_attribute_19,
   gold_order_filter::order_attribute_20 AS order_attribute_20,
   gold_order_filter::order_attribute_21 AS order_attribute_21,
   gold_order_filter::order_attribute_22 AS order_attribute_22,
   gold_order_filter::order_attribute_23 AS order_attribute_23,
   gold_order_filter::order_attribute_24 AS order_attribute_24,
   gold_order_filter::order_attribute_25 AS order_attribute_25,
   gold_order_filter::order_attribute_26 AS order_attribute_26,
   gold_order_filter::order_attribute_27 AS order_attribute_27,
   gold_order_filter::order_attribute_28 AS order_attribute_28,
   gold_order_filter::order_attribute_29 AS order_attribute_29,
   gold_order_filter::order_attribute_30 AS order_attribute_30,
   gold_order_filter::order_attribute_31 AS order_attribute_31,
   gold_order_filter::order_attribute_32 AS order_attribute_32,
   gold_order_filter::order_attribute_33 AS order_attribute_33,
   gold_order_filter::order_attribute_34 AS order_attribute_34,
   gold_order_filter::order_attribute_35 AS order_attribute_35,
   gold_order_filter::order_attribute_36 AS order_attribute_36,
   gold_order_filter::order_attribute_37 AS order_attribute_37,
   gold_order_filter::order_attribute_38 AS order_attribute_38,
   gold_order_filter::order_attribute_39 AS order_attribute_39,
   gold_order_filter::order_attribute_40 AS order_attribute_40,
   gold_order_filter::order_attribute_41 AS order_attribute_41,
   gold_order_filter::order_attribute_42 AS order_attribute_42,
   gold_order_filter::order_attribute_43 AS order_attribute_43,
   gold_order_filter::order_attribute_44 AS order_attribute_44,
   gold_order_filter::order_attribute_45 AS order_attribute_45,
   gold_order_filter::order_attribute_46 AS order_attribute_46,
   gold_order_filter::order_attribute_47 AS order_attribute_47,
   gold_order_filter::order_attribute_48 AS order_attribute_48,
   gold_order_filter::order_attribute_49 AS order_attribute_49,
   gold_order_filter::order_attribute_50 AS order_attribute_50,
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
   gold_order_filter::batch_id AS batch_id;
   
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_order table.
STORE generate_records
   --INTO '$GOLD_HDFS/$GOLD_ORDER/batch_id=$batch_id'
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_ORDER'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
