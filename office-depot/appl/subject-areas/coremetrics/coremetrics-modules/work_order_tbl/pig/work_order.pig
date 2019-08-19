--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_order      								   		               #
--# File                                                                       #
--#     : work_order.pig                                   	                   #
--# Description                                                                #
--#     : To load data into work_order table.              	                   #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$WORK_ORDER_SSH_ACTION';

--Loading data from gold_clickstream.gold_order table.
work_order = 
   LOAD '$DB_GOLD.$GOLD_ORDER'
   USING org.apache.hive.hcatalog.pig.HCatLoader();
   

--##############################################################################
--#                                 Transform                                  #
--##############################################################################
   

work_order_filter = 
	FILTER work_order BY (batch_id=='$batch_id' AND session_id IS NOT NULL);

work_order_generate =   
	FOREACH work_order_filter 
	GENERATE session_id AS session_id,
    order_id AS order_id, 
    order_total AS order_total, 
    visitor_segment AS visitor_segment,
    timestamp AS load_date,
    (customer_type IS NULL OR customer_type=='' OR customer_type==' ' ? '$DEF_VALUE':customer_type) AS customer_type,
    (country_description IS NULL OR country_description=='' OR country_description==' ' ? '$DEF_VALUE':country_description) AS country_description;
  
work_order_group = 
	GROUP work_order_generate BY (country_description,visitor_segment,load_date);
 
work_order_group_aggregate =  
	FOREACH work_order_group 
	GENERATE group, 
    COUNT_STAR(work_order_generate.order_id) AS total_orders,
    SUM(work_order_generate.order_total) AS total_revenue;
   
work_order_group_aggregate_flatten = 
	FOREACH work_order_group_aggregate 
	GENERATE FLATTEN($0), 
    total_orders, 
    total_revenue;
      
work_order_output= 
	FOREACH work_order_group_aggregate_flatten 
	GENERATE group::country_description AS country_description,
	group::visitor_segment AS visitor_segment,  
    total_orders AS total_orders,
    (visitor_segment=='$NEW_CUST'?total_revenue:0) AS total_revenue_from_new_customer,
    (visitor_segment=='$EXIST_CUST'?total_revenue:0) AS total_revenue_from_existing_customer,
    (((visitor_segment !='$NEW_CUST') AND (visitor_segment != '$EXIST_CUST')) ? total_revenue:0) AS total_revenue_from_anonymous_customer,      
    group::load_date AS load_date,
    '$batch_id' AS batch_id; 

      
--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_order_output 
   INTO '$DB_WORK.$WORK_ORDER' 
   USING org.apache.hive.hcatalog.pig.HCatStorer(); 
       

--##############################################################################
--#                                    End                                     #
--##############################################################################
