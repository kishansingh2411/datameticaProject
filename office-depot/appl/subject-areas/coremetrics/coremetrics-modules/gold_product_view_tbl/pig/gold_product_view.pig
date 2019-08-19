--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_product_view      								   			   #
--# File                                                                       #
--#     : gold_product_view.pig                                   			   #
--# Description                                                                #
--#     : To load data into gold_product_view table.              			   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_PRODUCT_VIEW_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_product_view table
gold_product_view = 
   LOAD '$DB_INCOMING.$INCOMING_PRODUCT_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_clickstream.work_geography_country_cookie table.
work_geography_country_cookie =
   LOAD '$DB_WORK.$WORK_GEOGRAPHY_COUNTRY_COOKIE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and header.
gold_product_view_filter = 
   FILTER gold_product_view BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
 
-- Filtering data for current batch_id.
work_geography_country_cookie_filter =
   FILTER work_geography_country_cookie BY (batch_id=='$batch_id');

-- Applying Left Outer Join on incoming_product_view and work_geography_country_cookie table.   
gold_product_view_geography_join = 
   JOIN gold_product_view_filter BY session_id LEFT OUTER, 
   work_geography_country_cookie_filter BY session_id;

-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
   FOREACH gold_product_view_geography_join GENERATE
		   (chararray)gold_product_view_filter::session_id AS session_id,
		   (chararray)gold_product_view_filter::cookie_id AS cookie_id,
		   gold_product_view_filter::timestamp AS time_stamp,
		   gold_product_view_filter::product_name AS product_name,
		   gold_product_view_filter::product_id AS product_id,
		   gold_product_view_filter::page_id AS page_id,
		   gold_product_view_filter::product_category_id AS product_category_id,
		   gold_product_view_filter::product_category AS product_category,
		   gold_product_view_filter::product_category_top AS product_category_top,
		   gold_product_view_filter::product_category_bottom AS product_category_bottom,
		   gold_product_view_filter::site_id AS site_id,
		   gold_product_view_filter::avg_star_rating AS avg_star_rating,
		   gold_product_view_filter::number_of_reviews AS number_of_reviews,
		   gold_product_view_filter::brand AS brand,
		   gold_product_view_filter::available_quantity AS available_quantity,
		   gold_product_view_filter::product_view_attribute_5 AS product_view_attribute_5,
		   gold_product_view_filter::product_view_attribute_6 AS product_view_attribute_6,
		   gold_product_view_filter::product_view_attribute_7 AS product_view_attribute_7,
		   gold_product_view_filter::product_view_attribute_8 AS product_view_attribute_8,
		   gold_product_view_filter::product_view_attribute_9 AS product_view_attribute_9,
		   gold_product_view_filter::effort_code AS effort_code,
		   gold_product_view_filter::site_promotion_tag AS site_promotion_tag,
		   gold_product_view_filter::availability_of_gallery_images AS availability_of_gallery_images,
		   gold_product_view_filter::discontinued_item AS discontinued_item,
		   gold_product_view_filter::product_view_attribute_14 AS product_view_attribute_14,
		   gold_product_view_filter::product_view_attribute_15 AS product_view_attribute_15,
		   gold_product_view_filter::tea_leaf_jsessionid AS tea_leaf_jsessionid,
		   gold_product_view_filter::style_id AS style_id,
		   gold_product_view_filter::style_description AS style_description,
		   gold_product_view_filter::product_view_attribute_19 AS product_view_attribute_19,
		   gold_product_view_filter::product_view_attribute_20 AS product_view_attribute_20,
		   gold_product_view_filter::product_view_attribute_21 AS product_view_attribute_21,
		   gold_product_view_filter::product_view_attribute_22 AS product_view_attribute_22,
		   gold_product_view_filter::product_view_attribute_23 AS product_view_attribute_23,
		   gold_product_view_filter::product_view_attribute_24 AS product_view_attribute_24,
		   gold_product_view_filter::product_view_attribute_25 AS product_view_attribute_25,
		   gold_product_view_filter::product_view_attribute_26 AS product_view_attribute_26,
		   gold_product_view_filter::product_view_attribute_27 AS product_view_attribute_27,
		   gold_product_view_filter::product_view_attribute_28 AS product_view_attribute_28,
		   gold_product_view_filter::product_view_attribute_29 AS product_view_attribute_29,
		   gold_product_view_filter::product_view_attribute_30 AS product_view_attribute_30,
		   gold_product_view_filter::product_view_attribute_31 AS product_view_attribute_31,
		   gold_product_view_filter::product_view_attribute_32 AS product_view_attribute_32,
		   gold_product_view_filter::product_view_attribute_33 AS product_view_attribute_33,
		   gold_product_view_filter::product_view_attribute_34 AS product_view_attribute_34,
		   gold_product_view_filter::product_view_attribute_35 AS product_view_attribute_35,
		   gold_product_view_filter::product_view_attribute_36 AS product_view_attribute_36,
		   gold_product_view_filter::product_view_attribute_37 AS product_view_attribute_37,
		   gold_product_view_filter::product_view_attribute_38 AS product_view_attribute_38,
		   gold_product_view_filter::product_view_attribute_39 AS product_view_attribute_39,
		   gold_product_view_filter::product_view_attribute_40 AS product_view_attribute_40,
		   gold_product_view_filter::product_view_attribute_41 AS product_view_attribute_41,
		   gold_product_view_filter::product_view_attribute_42 AS product_view_attribute_42,
		   gold_product_view_filter::product_view_attribute_43 AS product_view_attribute_43,
		   gold_product_view_filter::product_view_attribute_44 AS product_view_attribute_44,
		   gold_product_view_filter::product_view_attribute_45 AS product_view_attribute_45,
		   gold_product_view_filter::product_view_attribute_46 AS product_view_attribute_46,
		   gold_product_view_filter::product_view_attribute_47 AS product_view_attribute_47,
		   gold_product_view_filter::product_view_attribute_48 AS product_view_attribute_48,
		   gold_product_view_filter::product_view_attribute_49 AS product_view_attribute_49,
		   gold_product_view_filter::product_view_attribute_50 AS product_view_attribute_50,
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
		   gold_product_view_filter::batch_id AS batch_id;
		   
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_product_view table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_PRODUCT_VIEW/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_PRODUCT_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
