--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_site_promotion_click          								   #
--# File                                                                       #
--#     : gold_site_promotion_click.pig                                        #
--# Description                                                                #
--#     : To load data into gold_site_promotion_click                          #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_SITE_PROMOTION_CLICK_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_site_promotion_click table.
gold_site_promotion_click = 
   LOAD '$DB_INCOMING.$INCOMING_SITE_PROMOTION_CLICK' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_site_promotion_click_filter = 
   FILTER gold_site_promotion_click BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
 
-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
	FOREACH gold_site_promotion_click_filter GENERATE
	(chararray)session_id AS session_id,
	(chararray)cookie_id AS cookie_id,
	timestamp AS timestamp,   
	site_promotion_type AS site_promotion_type,
	site_promotion_promo AS site_promotion_promo,
	site_promotion_link AS site_promotion_link,
	site_promotion_page AS site_promotion_page,
	site_id AS site_id,
	batch_id AS batch_id;   


--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_site_promotion_click table.
STORE generate_records 
   --INTO '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_SITE_PROMOTION}/batch_id=$batch_id' 
  --INTO '$GOLD_HDFS/$GOLD_SITE_PROMOTION_CLICK/batch_id=$batch_id'
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_SITE_PROMOTION_CLICK'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
