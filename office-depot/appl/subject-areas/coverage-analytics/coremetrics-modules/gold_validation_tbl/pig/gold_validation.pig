 --##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_validation      								                   #
--# File                                                                       #
--#     : gold_validation.pig                                                  #
--# Description                                                                #
--#     : To load data into gold_validation table                              #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_VALIDATION_SSH_ACTION';
SET default_parallel 5;
-- Loading data from incoming_clickstream.incoming_validation table.
gold_validation = 
   LOAD '$DB_INCOMING.$INCOMING_VALIDATION' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_validation_filter = 
   FILTER gold_validation BY (batch_id=='$batch_id');

-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
	FOREACH gold_validation_filter GENERATE
	raw_file_name AS raw_file_name,
	record_count AS record_count,
	batch_id AS batch_id;   
	
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_validation table.
STORE generate_records 
  --INTO '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_VALIDATION}/batch_id=$batch_id' 
  --INTO '$GOLD_HDFS/$GOLD_VALIDATION/batch_id=$batch_id'
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_VALIDATION'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
