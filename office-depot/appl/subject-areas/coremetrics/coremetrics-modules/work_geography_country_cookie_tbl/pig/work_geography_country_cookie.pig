--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : gold_cart_item_abandonment      								   	   #
--# File                                                                       #
--#     : gold_cart_item_abandonment.pig                                   	   #
--# Description                                                                #
--#     : To load data into gold_cart_item_abandonment table.              	   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Sonali Rawool                         			   				   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$WORK_GEOGRAPHY_COOKIE_SSH_ACTION';
SET default_parallel 8;

-- Loading data from incoming_clickstream.incoming_geography table.
work_geography =
   LOAD '$DB_INCOMING.$INCOMING_GEOGRAPHY'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   
-- Loading data from incoming_clickstream.incoming_dim_country table.
work_dim_country = 
   LOAD '$DB_INCOMING.$INCOMING_DIM_COUNTRY'
   USING org.apache.hive.hcatalog.pig.HCatLoader();   
   
-- Loading data from incoming_clickstream.incoming_dim_cookie table.
work_dim_cookie = 
   LOAD '$DB_INCOMING.$INCOMING_DIM_COOKIES'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.   
work_geography_filter = 
   FILTER work_geography 
   BY (batch_id=='$batch_id' AND session_id != 'SESSION_ID');


-- Applying Left Outer Join on work_geography_filter and work_dim_country.   
work_geography_country_code_join = 
   JOIN work_geography_filter BY client_name LEFT OUTER, 
   work_dim_country BY client_name;

  
-- Applying Left Outer Join on work_geography_country_join and work_dim_cookie.  
 
work_geography_visitor_segmentation = 
   JOIN work_geography_country_code_join BY (work_geography_filter::cookie_id,work_dim_country::country_code) LEFT OUTER, 
   	 work_dim_cookie BY (cookie_id,country_code) ;

-- Generating required fields from work_geography_country_cookie_join table.
generate_records =
    FOREACH work_geography_visitor_segmentation
    GENERATE
        work_geography_country_code_join::work_geography_filter::session_id AS session_id,
        work_geography_country_code_join::work_geography_filter::cookie_id AS cookie_id,
        --(work_geography_country_code_join::work_geography_filter::cookie_id==''?'$NEW_CUST':'$EXIST_CUST') AS visitor_segment,
        ((work_dim_cookie::cookie_id=='' OR work_dim_cookie::cookie_id==' ' OR work_dim_cookie::cookie_id is NULL) ?'$NEW_CUST':'$EXIST_CUST') AS visitor_segment,
        work_geography_country_code_join::work_geography_filter::ip_address AS ip_address,
        work_geography_country_code_join::work_geography_filter::city AS city,
        work_geography_country_code_join::work_geography_filter::state AS state,
        work_geography_country_code_join::work_geography_filter::country AS country,
        work_geography_country_code_join::work_geography_filter::second_level_domain AS second_level_domain,
        work_geography_country_code_join::work_geography_filter::client_name AS client_name,
        work_geography_country_code_join::work_dim_country::country_code AS country_code,
        work_geography_country_code_join::work_dim_country::language AS language,
        work_geography_country_code_join::work_dim_country::country_description AS country_description,
        work_geography_country_code_join::work_geography_filter::dma AS dma,
        work_geography_country_code_join::work_geography_filter::batch_id AS batch_id;
        
generate_cookie_id=
    FOREACH generate_records
    GENERATE
     (visitor_segment=='$NEW_CUST'? cookie_id:'') AS cookie_id,
      country_code AS country_code;
      
generate_new_cookie_id = 
   FILTER generate_cookie_id 
   BY (cookie_id !='');    
      
       
--##############################################################################
--#                                   Store                                    #
--##############################################################################
-- storing cookie records into incoming_cookie_id path
STORE generate_new_cookie_id
   INTO '$COOKIE_HDFS/$batch_id/'
   USING PigStorage(',');

-- Storing records into work_clickstream.work_geography_country_cookie table.
STORE generate_records 
   INTO '$DB_WORK.$WORK_GEOGRAPHY_COUNTRY_COOKIE' 
   USING org.apache.hive.hcatalog.pig.HCatStorer('batch_id=$batch_id');
   


--##############################################################################
--#                                    End                                     #
--##############################################################################
