--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_registration      					            			   #
--# File                                                                       #
--#     : gold_registration.pig                                                #
--# Description                                                                #
--#     : To load data into gold_registration                                  #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$GOLD_REGISTRATION_SSH_ACTION';
SET default_parallel 5;

-- Loading data from incoming_clickstream.incoming_registration table.
gold_registration = 
   LOAD '$DB_INCOMING.$INCOMING_REGISTRATION' 
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.
gold_registration_filter = 
   FILTER gold_registration BY (batch_id == '$batch_id' AND session_id != 'SESSION_ID');

-- Generating required fields from incoming_product_view and work_geography_country_cookie table.
generate_records = 
	FOREACH gold_registration_filter GENERATE
	(chararray)session_id AS session_id,
	(chararray)cookie_id AS cookie_id,
	new_repeat_buyer AS new_repeat_buyer,
	new_repeat_visitor AS new_repeat_visitor,
	visitor_customer_flag AS visitor_customer_flag,
	registered_city AS registered_city,
	registered_state AS registered_state,
	registered_country AS registered_country,
	registered_zip_code AS registered_zip_code,
	gender AS gender,
	age AS age,
	reg1 AS reg1,
	reg2 AS reg2,
	reg3 AS reg3,
	reg4 AS reg4,
	reg5 AS reg5,
	reg6 AS reg6,
	reg7 AS reg7,
	reg8 AS reg8,
	reg9 AS reg9,
	reg10 AS reg10,
	registration_attribute_11 AS registration_attribute_11,
	account_type_name AS account_type_name,
	customer_type AS customer_type,
	customer_type_description AS customer_type_description,
	registration_15_test AS registration_15_test,
	user_store_id AS user_store_id,
	customer_id  AS customer_id ,
	customer_first_name AS customer_first_name,
	customer_last_name AS customer_last_name,
	company_name AS company_name,
	omxodp_acccount_merger_type AS omxodp_acccount_merger_type,
	coupon_code AS coupon_code,
	loyaltyid AS loyaltyid,
	loyaltyid_flag AS loyaltyid_flag,
	registration_attribute_25 AS registration_attribute_25,
	registration_attribute_26 AS registration_attribute_26,
	registration_attribute_27 AS registration_attribute_27,
	registration_attribute_28 AS registration_attribute_28,
	registration_attribute_29 AS registration_attribute_29,
	registration_attribute_30 AS registration_attribute_30,
	registration_attribute_31 AS registration_attribute_31,
	registration_attribute_32 AS registration_attribute_32,
	registration_attribute_33 AS registration_attribute_33,
	registration_attribute_34 AS registration_attribute_34,
	registration_attribute_35 AS registration_attribute_35,
	registration_attribute_36 AS registration_attribute_36,
	registration_attribute_37 AS registration_attribute_37,
	registration_attribute_38 AS registration_attribute_38,
	registration_attribute_39 AS registration_attribute_39,
	registration_attribute_40 AS registration_attribute_40,
	registration_attribute_41 AS registration_attribute_41,
	registration_attribute_42 AS registration_attribute_42,
	registration_attribute_43 AS registration_attribute_43,
	registration_attribute_44 AS registration_attribute_44,
	registration_attribute_45 AS registration_attribute_45,
	registration_attribute_46 AS registration_attribute_46,
	registration_attribute_47 AS registration_attribute_47,
	registration_attribute_48 AS registration_attribute_48,
	registration_attribute_49 AS registration_attribute_49,
	registration_attribute_50 AS registration_attribute_50,
	ToString(last_update_date,'yyyyMMdd') AS last_update_date,
	batch_id AS batch_id;   

--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into gold_clickstream.gold_registration table.
STORE generate_records 
   --INTO '$GOLD_HDFS/$GOLD_REGISTRATION/batch_id=$batch_id' 
   --USING parquet.pig.ParquetStorer;
   INTO '$DB_GOLD.$GOLD_REGISTRATION'
   USING org.apache.hive.hcatalog.pig.HCatStorer();
--##############################################################################
--#                                    End                                     #
--##############################################################################
