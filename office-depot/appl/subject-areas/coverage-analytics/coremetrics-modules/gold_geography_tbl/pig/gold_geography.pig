--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_geography                    								   #
--# File                                                                       #
--#     : gold_geography.pig                                                   #
--# Description                                                                #
--#     : To load data into gold_geography                                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_GEOGRAPHY_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_geography table.
gold_geography = 
   LOAD '$DB_INCOMING.$INCOMING_GEOGRAPHY' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_geography_filter = FILTER gold_geography BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');

-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
	FOREACH gold_geography_filter GENERATE
	(chararray)session_id AS session_id,
	(chararray)cookie_id AS cookie_id,
	ip_address AS ip_address,
	city AS city,
	state AS state,
	country AS country,
	dma AS dma,
	second_level_domain AS second_level_domain,
	batch_id AS batch_id;   


--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_geography table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_GEOGRAPHY/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_GEOGRAPHY'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
