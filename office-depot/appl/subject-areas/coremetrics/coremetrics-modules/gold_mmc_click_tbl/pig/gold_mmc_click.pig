--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_mmc_click                     								   #
--# File                                                                       #
--#     : gold_mmc_click.pig                                                   #
--# Description                                                                #
--#     : To load data into gold_mmc_click                                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_MMC_CLICK_SSH_ACTION';
SET default_parallel 5;
-- Loading data from incoming_clickstream.incoming_mmc_click table.
gold_mmc_click = 
   LOAD '$DB_INCOMING.$INCOMING_MMC_CLICK' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_mmc_click_filter = 
   FILTER gold_mmc_click BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
   
   -- Generating required fields from incoming_product_view and work_geography_country_cookie table.
 generate_records = 
	FOREACH gold_mmc_click_filter GENERATE
	(chararray)session_id AS session_id,
	(chararray)cookie_id AS cookie_id, 
	timestamp AS time_stamp,  
	marketing_vendor AS marketing_vendor,
	marketing_category AS marketing_category,
	marketing_placement AS marketing_placement,
	marketing_item AS marketing_item,
	site_id AS site_id,
	mmc_click_attribute_1 AS mmc_click_attribute_1,
	mmc_click_attribute_2 AS mmc_click_attribute_2,
	mmc_click_attribute_3 AS mmc_click_attribute_3,
	mmc_click_attribute_4 AS mmc_click_attribute_4,
	mmc_click_attribute_5 AS mmc_click_attribute_5,
	mmc_click_attribute_6 AS mmc_click_attribute_6,
	mmc_click_attribute_7 AS mmc_click_attribute_7,
	mmc_click_attribute_8 AS mmc_click_attribute_8,
	mmc_click_attribute_9 AS mmc_click_attribute_9,
	mmc_click_attribute_10 AS mmc_click_attribute_10,
	mmc_click_attribute_11 AS mmc_click_attribute_11,
	mmc_click_attribute_12 AS mmc_click_attribute_12,
	mmc_click_attribute_13 AS mmc_click_attribute_13,
	mmc_click_attribute_14 AS mmc_click_attribute_14,
	mmc_click_attribute_15 AS mmc_click_attribute_15,
	mmc_click_attribute_16 AS mmc_click_attribute_16,
	mmc_click_attribute_17 AS mmc_click_attribute_17,
	mmc_click_attribute_18 AS mmc_click_attribute_18,
	mmc_click_attribute_19 AS mmc_click_attribute_19,
	mmc_click_attribute_20 AS mmc_click_attribute_20,
	mmc_click_attribute_21 AS mmc_click_attribute_21,
	mmc_click_attribute_22 AS mmc_click_attribute_22,
	mmc_click_attribute_23 AS mmc_click_attribute_23,
	mmc_click_attribute_24 AS mmc_click_attribute_24,
	mmc_click_attribute_25 AS mmc_click_attribute_25,
	mmc_click_attribute_26 AS mmc_click_attribute_26,
	mmc_click_attribute_27 AS mmc_click_attribute_27,
	mmc_click_attribute_28 AS mmc_click_attribute_28,
	mmc_click_attribute_29 AS mmc_click_attribute_29,
	mmc_click_attribute_30 AS mmc_click_attribute_30,
	mmc_click_attribute_31 AS mmc_click_attribute_31,
	mmc_click_attribute_32 AS mmc_click_attribute_32,
	mmc_click_attribute_33 AS mmc_click_attribute_33,
	mmc_click_attribute_34 AS mmc_click_attribute_34,
	mmc_click_attribute_35 AS mmc_click_attribute_35,
	mmc_click_attribute_36 AS mmc_click_attribute_36,
	mmc_click_attribute_37 AS mmc_click_attribute_37,
	mmc_click_attribute_38 AS mmc_click_attribute_38,
	mmc_click_attribute_39 AS mmc_click_attribute_39,
	mmc_click_attribute_40 AS mmc_click_attribute_40,
	mmc_click_attribute_41 AS mmc_click_attribute_41,
	mmc_click_attribute_42 AS mmc_click_attribute_42,
	mmc_click_attribute_43 AS mmc_click_attribute_43,
	mmc_click_attribute_44 AS mmc_click_attribute_44,
	mmc_click_attribute_45 AS mmc_click_attribute_45,
	mmc_click_attribute_46 AS mmc_click_attribute_46,
	mmc_click_attribute_47 AS mmc_click_attribute_47,
	mmc_click_attribute_48 AS mmc_click_attribute_48,
	mmc_click_attribute_49 AS mmc_click_attribute_49,
	mmc_click_attribute_50 AS mmc_click_attribute_50,
	batch_id AS batch_id;
	
   

--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_mmc_click table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_MMC_CLICK/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_MMC_CLICK'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
