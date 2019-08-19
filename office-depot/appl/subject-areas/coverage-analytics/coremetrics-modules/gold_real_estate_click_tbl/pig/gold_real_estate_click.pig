	--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_real_estate_click              								   #
--# File                                                                       #
--#     : gold_real_estate_click.pig                                           #
--# Description                                                                #
--#     : To load data into gold_real_estate_click                             #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_REAL_ESTATE_CLICK_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_real_estate_click table.
gold_real_estate_click = 
   LOAD '$DB_INCOMING.$INCOMING_REAL_ESTATE_CLICK' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_real_estate_click_filter = 
   FILTER gold_real_estate_click BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
   
   -- Generating required fields from incoming_product_view and work_geography_country_cookie table.
   generate_records = 
	FOREACH gold_real_estate_click_filter GENERATE
	(chararray)session_id AS session_id,
	(chararray)cookie_id AS cookie_id, 
	timestamp AS timestamp,
	real_estate_page AS real_estate_page,
	real_estate_version AS real_estate_version,
	real_estate_page_area AS real_estate_page_area,
	real_estate_link AS real_estate_link,
	site_id AS site_id,
	batch_id AS batch_id;
   

--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_real_estate_click table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_REAL_ESTATE_CLICK/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_REAL_ESTATE_CLICK'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
