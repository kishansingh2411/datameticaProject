--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_technical_properties      								   #
--# File                                                                       #
--#     : gold_technical_properties.pig                                   #
--# Description                                                                #
--#     : To load data into gold_technical_properties                         #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_TECHNICAL_PROPERTIES_SSH_ACTION';
SET default_parallel 5;
-- Loading data from incoming_clickstream.incoming_technical_properties table.
gold_technical_properties = 
   LOAD '$DB_INCOMING.$INCOMING_TECHNICAL_PROPERTIES' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_technical_properties_filter = 
   FILTER gold_technical_properties BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');
   
-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
	FOREACH gold_technical_properties_filter GENERATE
	(chararray)session_id AS session_id,
	(chararray)cookie_id AS cookie_id,
	timestamp AS time_stamp,
    browser_type AS browser_type,   
	javascript_version AS javascript_version,
	language AS language,
	screen_resolution AS screen_resolution,
	color_depth AS color_depth,
	operating_system AS operating_system,
	time_zone AS time_zone,
	mobile_network AS mobile_network,
	connection_type AS connection_type,
	connection_speed AS connection_speed,
	is_mobile_device AS is_mobile_device,
	mobile_device AS mobile_device,
	device_marketing_name AS device_marketing_name,
	device_model AS device_model,
	device_type AS device_type,
	device_vendor AS device_vendor,
	cookie_support AS cookie_support,
	flash_support AS flash_support,
	touch_screen AS touch_screen,
	video_3gp_support AS video_3gp_support,
	video_mp4_support AS video_mp4_support,
	video_wmv_support AS video_wmv_support,
	java_enabled AS java_enabled,
	batch_id AS batch_id;   


--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_technical_properties table.
STORE generate_records 
   --INTO '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_TECHNICAL_PROPERTIES}/batch_id=$batch_id' 
   --INTO '$GOLD_HDFS/$GOLD_TECHNICAL_PROPERTIES/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_TECHNICAL_PROPERTIES'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
