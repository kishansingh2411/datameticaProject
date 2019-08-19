--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_page_view                                                       #
--# File                                                                       #
--#     : gold_page_view.pig                                                   #
--# Description                                                                #
--#     : To load data into gold_page_view table.                              #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                                     #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_PAGE_VIEW_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_page_view table
gold_page_view =
   LOAD '$DB_INCOMING.$INCOMING_PAGE_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

-- Loading data from work_clickstream.work_geography_country_cookie table.
work_geography_country_cookie =
   LOAD '$DB_WORK.$WORK_GEOGRAPHY_COUNTRY_COOKIE'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and header.
gold_page_view_filter =
   FILTER gold_page_view BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');

-- Filtering data for current batch_id.
work_geography_country_cookie_filter =
   FILTER work_geography_country_cookie BY (batch_id=='$batch_id');
      
-- Applying Left Outer Join on incoming_page_view and work_geography_country_cookie table.
gold_page_view_geography_join =
   JOIN gold_page_view_filter BY session_id LEFT OUTER,
   work_geography_country_cookie_filter BY session_id;

-- Generating required fields from incoming_page_view and work_geography_country_cookie table.
generate_records =
   FOREACH 
   gold_page_view_geography_join GENERATE
	   (chararray)gold_page_view_filter::session_id AS session_id,
	   (chararray)gold_page_view_filter::cookie_id AS cookie_id,
	   gold_page_view_filter::timestamp AS time_stamp,
	   gold_page_view_filter::page AS page,
	   gold_page_view_filter::page_id AS page_id,
	   gold_page_view_filter::content_category AS content_category,
	   gold_page_view_filter::content_category_id AS content_category_id,
	   gold_page_view_filter::content_category_top AS content_category_top,
	   gold_page_view_filter::content_category_bottom AS content_category_bottom,
	   gold_page_view_filter::on_site_search_term AS on_site_search_term,
	   gold_page_view_filter::page_url AS page_url,
	   gold_page_view_filter::page_referral_url AS page_referral_url,
	   gold_page_view_filter::site_id AS site_id,
	   gold_page_view_filter::browse_type_search_type AS browse_type_search_type,
	   gold_page_view_filter::list_grid_view AS list_grid_view,
	   gold_page_view_filter::sort_state AS sort_state,
	   gold_page_view_filter::sort_size AS sort_size,
	   gold_page_view_filter::app_version AS app_version,
	   gold_page_view_filter::app_device_name AS app_device_name,
	   gold_page_view_filter::app_os AS app_os,
	   gold_page_view_filter::b2b_b2c AS b2b_b2c,
	   gold_page_view_filter::ink_and_toner_search AS ink_and_toner_search,
	   gold_page_view_filter::onsite_search_term AS onsite_search_term,
	   gold_page_view_filter::refinement_selection AS refinement_selection,
	   gold_page_view_filter::search_refinement_category AS search_refinement_category,
	   gold_page_view_filter::search_refinement_item AS search_refinement_item,
	   gold_page_view_filter::search_refinement_combination AS search_refinement_combination,
	   gold_page_view_filter::original_search_term AS original_search_term,
	   gold_page_view_filter::tea_leaf_jsessionid AS tea_leaf_jsessionid,
	   gold_page_view_filter::related_search_terms AS related_search_terms,
	   gold_page_view_filter::account_source AS account_source,
	   gold_page_view_filter::customerid AS customerid,
	   gold_page_view_filter::account_type AS account_type,
	   gold_page_view_filter::page_view_attribute_21 AS page_view_attribute_21,
	   gold_page_view_filter::page_view_attribute_22 AS page_view_attribute_22,
	   gold_page_view_filter::page_view_attribute_23 AS page_view_attribute_23,
	   gold_page_view_filter::page_view_attribute_24 AS page_view_attribute_24,
	   gold_page_view_filter::page_view_attribute_25 AS page_view_attribute_25,
	   gold_page_view_filter::page_view_attribute_26 AS page_view_attribute_26,
	   gold_page_view_filter::page_view_attribute_27 AS page_view_attribute_27,
	   gold_page_view_filter::page_view_attribute_28 AS page_view_attribute_28,
	   gold_page_view_filter::page_view_attribute_29 AS page_view_attribute_29,
	   gold_page_view_filter::page_view_attribute_30 AS page_view_attribute_30,
	   gold_page_view_filter::page_view_attribute_31 AS page_view_attribute_31,
	   gold_page_view_filter::page_view_attribute_32 AS page_view_attribute_32,
	   gold_page_view_filter::page_view_attribute_33 AS page_view_attribute_33,
	   gold_page_view_filter::page_view_attribute_34 AS page_view_attribute_34,
	   gold_page_view_filter::page_view_attribute_35 AS page_view_attribute_35,
	   gold_page_view_filter::page_view_attribute_36 AS page_view_attribute_36,
	   gold_page_view_filter::page_view_attribute_37 AS page_view_attribute_37,
	   gold_page_view_filter::page_view_attribute_38 AS page_view_attribute_38,
	   gold_page_view_filter::page_view_attribute_39 AS page_view_attribute_39,
	   gold_page_view_filter::page_view_attribute_40 AS page_view_attribute_40,
	   gold_page_view_filter::page_view_attribute_41 AS page_view_attribute_41,
	   gold_page_view_filter::page_view_attribute_42 AS page_view_attribute_42,
	   gold_page_view_filter::page_view_attribute_43 AS page_view_attribute_43,
	   gold_page_view_filter::page_view_attribute_44 AS page_view_attribute_44,
	   gold_page_view_filter::page_view_attribute_45 AS page_view_attribute_45,
	   gold_page_view_filter::page_view_attribute_46 AS page_view_attribute_46,
	   gold_page_view_filter::page_view_attribute_47 AS page_view_attribute_47,
	   gold_page_view_filter::page_view_attribute_48 AS page_view_attribute_48,
	   gold_page_view_filter::page_view_attribute_49 AS page_view_attribute_49,
	   gold_page_view_filter::page_view_attribute_50 AS page_view_attribute_50,
	   (chararray)gold_page_view_filter::search_result_count AS search_result_count,
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
	   gold_page_view_filter::batch_id AS batch_id;
   
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_page_view table.
STORE generate_records
   --INTO '$GOLD_HDFS/$GOLD_PAGE_VIEW/batch_id=$batch_id'
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_PAGE_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
