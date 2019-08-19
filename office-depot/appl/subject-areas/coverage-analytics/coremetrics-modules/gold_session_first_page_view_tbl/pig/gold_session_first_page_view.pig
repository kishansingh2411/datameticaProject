--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_session_first_page_view              						   #
--# File                                                                       #
--#     : gold_session_first_page_view.pig                                     #
--# Description                                                                #
--#     : To load data into gold_session_first_page_view                       #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_SESSION_FIRST_PAGE_VIEW_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_session_first_page_view table.
gold_session_first_page_view = 
   LOAD '$DB_INCOMING.$INCOMING_SESSION_FIRST_PAGE_VIEW' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_session_first_page_view_filter = 
   FILTER gold_session_first_page_view BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
   
-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records =
    FOREACH gold_session_first_page_view_filter GENERATE
	(chararray)session_id AS session_id,
	(chararray)cookie_id AS cookie_id,
	timestamp AS first_timestamp,
	first_referring_url AS first_referring_url,
	first_destination_url AS first_destination_url,
	first_referral_type AS first_referral_type,
	ip_address AS ip_address,
	timestamp AS timestamp,
	referral_name AS referral_name,
	referring_url AS referring_url,
	referral_type AS referral_type,
	natural_search_term AS natural_search_term,
	destination_url AS destination_url,
	user_agent AS user_agent,
	search_engine AS search_engine,
	marketing_vendor AS marketing_vendor,
	marketing_category AS marketing_category,
	marketing_placement AS marketing_placement,
	marketing_item AS marketing_item,
	(chararray)visitor_ad_impression_id AS visitor_ad_impression_id,
	batch_id AS batch_id; 


--##################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_session_first_page_view table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_SESSION_FIRST_PAGE_VIEW/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_SESSION_FIRST_PAGE_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
