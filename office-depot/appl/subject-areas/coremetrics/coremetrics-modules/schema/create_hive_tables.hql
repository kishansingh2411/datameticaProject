--#######################################################################################
--#                              General Details                               		    #
--#######################################################################################
--#                                                                            		    #
--# Name                                                                                #
--#     : create_hive_tables      								   	 		            #
--# File                                                                                #
--#     : create_hive_tables.hql                                   	                    #
--# Description                                                                         #
--#     : Contains ddl for Hive Schema 		  									        #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--# Author                                                                              #
--#     : Deepanshu and Shweta                			 					            #
--#                                                                                     #
--#######################################################################################


--Creating databases

CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_INCOMING};

CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_GOLD};

CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_WORK};

CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_AUDIT};


--# Creating incoming layer tables 

--# incoming_dim_cookies

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_DIM_COOKIES}
(
  cookie_id			string,
  country_code		string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_DIM_COOKIES}'
tblproperties ("skip.header.line.count"="1");

--##############################################################################

--# Creating incoming_dim_country table

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_DIM_COUNTRY}
(
  client_name       string,
  country_code      string,
  language      	string,
  country_description      	string
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_DIM_COUNTRY}';



--# incoming_cart_item_abandonment

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ABANDONMENT}
(
  session_id               string,
  cookie_id                string,
  timestamp                timestamp,
  product_id               string,
  product_name             string,
  product_category         string,
  product_category_id      string,
  product_category_top     string,
  product_category_bottom  string,
  base_price               float,
  quantity                 decimal(8,0),
  order_id                 string,
  site_id                  string,
  delivery_mode            string,
  avg_rating               string,
  total_review_count       string,
  brand_code               string,
  product_avaliability     string,
  promo_type               string,
  shipping_description     string,
  abandonment_attribute_8  string,
  abandonment_attribute_9  string,
  b2b_b2c                  string,
  abandonment_attribute_11 string,
  effort_code              string,
  discount_total           string,
  abandonment_attribute_14 string,
  atc_source               string,
  list_source              string,
  ssc_list_grade           string,
  atc_location             string,
  atc_referral_pid         string,
  atc_referral_url         string,
  shop_21                  string,
  style_id                 string,
  style_description        string,
  abandonment_attribute_24 string,
  abandonment_attribute_25 string,
  abandonment_attribute_26 string, 
  abandonment_attribute_27 string,
  abandonment_attribute_28 string,
  abandonment_attribute_29 string,
  abandonment_attribute_30 string,
  abandonment_attribute_31 string,
  abandonment_attribute_32 string,
  abandonment_attribute_33 string,
  abandonment_attribute_34 string,
  abandonment_attribute_35 string,
  abandonment_attribute_36 string,
  abandonment_attribute_37 string,
  abandonment_attribute_38 string,
  abandonment_attribute_39 string,
  abandonment_attribute_40 string,
  abandonment_attribute_41 string,
  abandonment_attribute_42 string,
  abandonment_attribute_43 string,
  abandonment_attribute_44 string,
  abandonment_attribute_45 string,
  abandonment_attribute_46 string,
  abandonment_attribute_47 string,
  abandonment_attribute_48 string,
  abandonment_attribute_49 string,
  abandonment_attribute_50 string 
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ABANDONMENT}'
tblproperties ("skip.header.line.count"="1");

--# incoming_cart_item_addition

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ADDITION}
(
  session_id              string,
  cookie_id               string,
  timestamp               timestamp,
  product_id              string,
  product_name            string,
  product_category        string,
  product_category_id     string,
  product_category_top    string,
  product_category_bottom string,
  base_price              float,
  quantity                decimal(8,0),
  order_id				  string,
  site_id                 string,
  delivery_mode           string,
  avg_rating              string,
  total_review_count      string,
  brand_code              string,
  product_avaliability    string,
  promo_type              string,
  shipping_description    string,
  cart_attribute_8        string,
  cart_attribute_9        string,
  b2b_b2c                 string,
  cart_attribute_11       string,
  effort_code             string,
  discount_total          string,
  cart_attribute_14       string,
  atc_source              string,
  list_source             string,
  ssc_list_grade          string,
  atc_location            string,
  atc_referral_pid        string,
  atc_referral_url        string,
  shop_21                 string,
  style_id                string,
  style_description       string, 
  cart_attribute_24       string,
  cart_attribute_25       string,
  cart_attribute_26       string,
  cart_attribute_27       string,
  cart_attribute_28       string,
  cart_attribute_29       string,
  cart_attribute_30       string,
  cart_attribute_31       string,
  cart_attribute_32       string,
  cart_attribute_33       string,
  cart_attribute_34       string,
  cart_attribute_35       string,
  cart_attribute_36       string,
  cart_attribute_37       string,
  cart_attribute_38       string,
  cart_attribute_39       string, 
  cart_attribute_40       string,
  cart_attribute_41       string,
  cart_attribute_42       string,
  cart_attribute_43       string,
  cart_attribute_44       string,
  cart_attribute_45       string,
  cart_attribute_46       string,
  cart_attribute_47       string,
  cart_attribute_48       string,
  cart_attribute_49       string,
  cart_attribute_50       string  
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ADDITION}'
tblproperties ("skip.header.line.count"="1");

--# incoming_cart_item_purchase

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_PURCHASE}
(
  session_id              string,
  cookie_id               string,
  timestamp               timestamp,
  product_id              string,
  product_name            string,
  product_category        string,
  product_category_id     string,
  product_category_top    string,
  product_category_bottom string,
  base_price              float,
  quantity                decimal(8,0),
  order_id                string,
  site_id                 string,
  delivery_mode           string,
  avg_rating              string,
  total_review_count      string,
  brand_code              string,  
  product_avaliability    string,
  promo_type              string,
  shipping_description    string,
  purchase_attribute_8    string,  
  purchase_attribute_9    string,
  b2b_b2c                 string,
  purchase_attribute_11   string,
  effort_code             string,
  discount_total          string,
  purchase_attribute_14   string,
  atc_source              string, 
  list_source             string,
  ssc_list_grade          string,
  atc_location            string,
  atc_referral_pid        string,
  atc_referral_url        string,
  shop_21                 string,
  style_id                string,
  style_description       string,
  purchase_attribute_24   string,
  purchase_attribute_25   string, 
  purchase_attribute_26   string,
  purchase_attribute_27   string,
  purchase_attribute_28   string,
  purchase_attribute_29   string, 
  purchase_attribute_30   string,
  purchase_attribute_31   string,
  purchase_attribute_32   string,
  purchase_attribute_33   string,
  purchase_attribute_34   string,
  purchase_attribute_35   string, 
  purchase_attribute_36   string,
  purchase_attribute_37   string,
  purchase_attribute_38   string,
  purchase_attribute_39   string,
  purchase_attribute_40   string,
  purchase_attribute_41   string,
  purchase_attribute_42   string,
  purchase_attribute_43   string,
  purchase_attribute_44   string,
  purchase_attribute_45   string,
  purchase_attribute_46   string,
  purchase_attribute_47   string,
  purchase_attribute_48   string,
  purchase_attribute_49   string,
  purchase_attribute_50   string 
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_PURCHASE}'
tblproperties ("skip.header.line.count"="1");


--# incoming_conversion_event

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_CONVERSION}
(
  session_id                    string,
  cookie_id                     string,
  timestamp                     timestamp,
  event_name                    string,
  event_category                string,
  event_action_type             decimal(2,0),
  event_points                  decimal(22,0),
  site_id                       string,
  ce_ex1_smart_cart_user_id     string,
  ce_ex2_smart_cart_account     string,
  ce_ex3_smartcartoriginal_sku  string,
  ce_ex4_smart_cart_original_q  string,
  ce_ex5_smart_cart_original_p  string,
  ce_ex6_smartcartpurchased_sku string,
  ce_ex7_smartcartpurchased_q   string,
  ce_ex8_smartcartpurchased_p   string, 
  ce_ex9_smart_cart_savings     string,
  ce_ex10_signup_city           string,
  ce_ex11_signup_state          string,
  ce_ex12_signup_number_of_emp  string,
  ce_ex13_signup_bus_industry   string,
  ce_ex14_signup_title          string, 
  subscription_sku              string,
  conversion_event_attribute_16 string,
  conversion_event_attribute_17 string,
  conversion_event_attribute_18 string,
  conversion_event_attribute_19 string,
  conversion_event_attribute_20 string,
  conversion_event_attribute_21 string,
  conversion_event_attribute_22 string,
  conversion_event_attribute_23 string,
  conversion_event_attribute_24 string, 
  conversion_event_attribute_25 string,
  conversion_event_attribute_26 string,
  conversion_event_attribute_27 string,
  conversion_event_attribute_28 string,
  conversion_event_attribute_29 string,
  conversion_event_attribute_30 string,
  conversion_event_attribute_31 string,
  conversion_event_attribute_32 string,
  conversion_event_attribute_33 string,
  conversion_event_attribute_34 string,
  conversion_event_attribute_35 string, 
  conversion_event_attribute_36 string,
  conversion_event_attribute_37 string,
  conversion_event_attribute_38 string, 
  conversion_event_attribute_39 string,
  conversion_event_attribute_40 string,
  conversion_event_attribute_41 string,
  conversion_event_attribute_42 string,
  conversion_event_attribute_43 string,
  conversion_event_attribute_44 string,
  conversion_event_attribute_45 string,
  conversion_event_attribute_46 string,
  conversion_event_attribute_47 string,
  conversion_event_attribute_48 string,
  conversion_event_attribute_49 string,
  conversion_event_attribute_50 string 
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_CONVERSION}'
tblproperties ("skip.header.line.count"="1");


--# incoming_element

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ELEMENT}
(
  session_id                          string,
  cookie_id                           string,
  timestamp                           timestamp,
  element_name                        string,
  element_category                    string,
  site_id                             string,
  product_page_sku                    string,
  endecca_refinement_category         string,
  endecca_refinement                  string, 
  error_messaging_product             string,
  endecca_refinement_product_category string,
  csl_add_to_list_from_location       string,
  csl_add_to_list_type                string,
  csl_create_list_from                string,
  csl_manage_list_link_from           string,
  csl_paginate_depth_lb_pg_num        string,
  csl_paginate_from                   string,
  csl_set_default_list_from           string,
  csl_sort_list_by                    string,
  csl_sort_list_from                  string,
  grand_total                         string,
  subscription_sku                    string,
  monetate_element                    string, 
  test_exposure                       string,
  local_browse_search_term            string,
  tab_name                            string,
  order_hist_page_srch_item           string,
  order_hist_page_srch_ord_type       string,
  order_hist_pg_srch_ord_status       string,
  order_hist_pg_prod_grp_filter       string,
  order_hist_pg_prod_cat_filter       string,
  order_hist_pg_sub_cat_filter        string,
  video_status                        string,
  video_time_stamp                    string,
  video_length                        string,
  predictive_test_painted_result      string,
  predictive_test_search_term         string,
  element_attribute_32                string,
  element_attribute_33                string,
  element_attribute_34                string,
  element_attribute_35                string,
  element_attribute_36                string,
  element_attribute_37                string,
  element_attribute_38                string,
  element_attribute_39                string,
  element_attribute_40                string,
  element_attribute_41                string,
  element_attribute_42                string,
  element_attribute_43                string,
  element_attribute_44                string,
  element_attribute_45                string,
  element_attribute_46                string,
  element_attribute_47                string, 
  element_attribute_48                string,
  element_attribute_49                string,
  element_attribute_50                string 
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ELEMENT}'
tblproperties ("skip.header.line.count"="1");


--# incoming_geography

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_GEOGRAPHY}
(
  session_id          string,
  cookie_id           string,
  ip_address          string,
  city                string,
  state               string,
  country             string,
  dma                 string,
  second_level_domain string  
)
PARTITIONED BY (batch_id string , client_name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_GEOGRAPHY}'
tblproperties ("skip.header.line.count"="1");


--# incoming_mmc_click

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_MMC_CLICK}
(
  session_id             string,
  cookie_id              string,
  timestamp              timestamp,
  marketing_vendor       string,
  marketing_category     string,
  marketing_placement    string,
  marketing_item         string,
  site_id                string,
  mmc_click_attribute_1  string,
  mmc_click_attribute_2  string,
  mmc_click_attribute_3  string,
  mmc_click_attribute_4  string,
  mmc_click_attribute_5  string,
  mmc_click_attribute_6  string,
  mmc_click_attribute_7  string,
  mmc_click_attribute_8  string,
  mmc_click_attribute_9  string,
  mmc_click_attribute_10 string,
  mmc_click_attribute_11 string,
  mmc_click_attribute_12 string,
  mmc_click_attribute_13 string,
  mmc_click_attribute_14 string,
  mmc_click_attribute_15 string,
  mmc_click_attribute_16 string,
  mmc_click_attribute_17 string,
  mmc_click_attribute_18 string,
  mmc_click_attribute_19 string,
  mmc_click_attribute_20 string,
  mmc_click_attribute_21 string, 
  mmc_click_attribute_22 string,
  mmc_click_attribute_23 string,
  mmc_click_attribute_24 string,
  mmc_click_attribute_25 string,
  mmc_click_attribute_26 string,
  mmc_click_attribute_27 string,
  mmc_click_attribute_28 string,
  mmc_click_attribute_29 string,
  mmc_click_attribute_30 string,
  mmc_click_attribute_31 string,
  mmc_click_attribute_32 string,
  mmc_click_attribute_33 string,
  mmc_click_attribute_34 string,
  mmc_click_attribute_35 string,
  mmc_click_attribute_36 string,
  mmc_click_attribute_37 string,
  mmc_click_attribute_38 string,
  mmc_click_attribute_39 string,
  mmc_click_attribute_40 string,
  mmc_click_attribute_41 string,
  mmc_click_attribute_42 string,
  mmc_click_attribute_43 string,
  mmc_click_attribute_44 string,
  mmc_click_attribute_45 string,
  mmc_click_attribute_46 string,
  mmc_click_attribute_47 string,
  mmc_click_attribute_48 string,
  mmc_click_attribute_49 string,
  mmc_click_attribute_50 string
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_MMC_CLICK}'
tblproperties ("skip.header.line.count"="1");


--# incoming_order

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ORDER}
(
  session_id               string,
  cookie_id                string,
  timestamp                timestamp,
  order_id                 string, 
  registration_id          string,
  order_total              float,
  shipping                 float,
  site_id                  string,
  delivery_mode            string,
  account_type             string,
  customer_type_b2b_or_b2c string,
  customer_type            string,
  order_attribute_5        string,
  order_attribute_6        string,
  order_attribute_7        string,
  coupons_total            string,
  association_discount     string,
  unknown_data_field       string,
  tender_type_credit_card  string,
  tender_type_paypal       string,
  tender_type_gift_card    string,
  coupon_code              string,
  loyaltyid                string,
  order_attribute_16       string,
  order_attribute_17       string,
  order_attribute_18       string,
  order_attribute_19       string,
  order_attribute_20       string,
  order_attribute_21       string,
  order_attribute_22       string,
  order_attribute_23       string,
  order_attribute_24       string,
  order_attribute_25       string,
  order_attribute_26       string,
  order_attribute_27       string,
  order_attribute_28       string,
  order_attribute_29       string,
  order_attribute_30       string,
  order_attribute_31       string,
  order_attribute_32       string,
  order_attribute_33       string,
  order_attribute_34       string,
  order_attribute_35       string,
  order_attribute_36       string,
  order_attribute_37       string,
  order_attribute_38       string,
  order_attribute_39       string,
  order_attribute_40       string,
  order_attribute_41       string,
  order_attribute_42       string,
  order_attribute_43       string,
  order_attribute_44       string,
  order_attribute_45       string,
  order_attribute_46       string,
  order_attribute_47       string,
  order_attribute_48       string,
  order_attribute_49       string,
  order_attribute_50       string
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ORDER}'
tblproperties ("skip.header.line.count"="1");


--# incoming_page_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_PAGE_VIEW}
(
  session_id                    string,
  cookie_id                     string,
  timestamp                     timestamp,
  page                          string,
  page_id                       string,
  content_category              string,
  content_category_id           string,
  content_category_top          string,
  content_category_bottom       string,
  on_site_search_term           string,
  page_url                      string,
  page_referral_url             string,
  site_id                       string,
  browse_type_search_type       string,
  list_grid_view                string,
  sort_state                    string,
  sort_size                     string,
  app_version                   string,
  app_device_name               string,
  app_os                        string,
  b2b_b2c                       string,
  ink_and_toner_search          string,
  onsite_search_term            string,
  refinement_selection          string,
  search_refinement_category    string,
  search_refinement_item        string,
  search_refinement_combination string,
  original_search_term          string,
  tea_leaf_jsessionid           string,
  related_search_terms          string,
  account_source                string,
  customerid                    string,
  account_type                  string,
  page_view_attribute_21        string,
  page_view_attribute_22        string,
  page_view_attribute_23        string,
  page_view_attribute_24        string,
  page_view_attribute_25        string,
  page_view_attribute_26        string,
  page_view_attribute_27        string,
  page_view_attribute_28        string,
  page_view_attribute_29        string,
  page_view_attribute_30        string,
  page_view_attribute_31        string,
  page_view_attribute_32        string,
  page_view_attribute_33        string,
  page_view_attribute_34        string,
  page_view_attribute_35        string,
  page_view_attribute_36        string,
  page_view_attribute_37        string,
  page_view_attribute_38        string,
  page_view_attribute_39        string,
  page_view_attribute_40        string,
  page_view_attribute_41        string,
  page_view_attribute_42        string,
  page_view_attribute_43        string,
  page_view_attribute_44        string,
  page_view_attribute_45        string,
  page_view_attribute_46        string,
  page_view_attribute_47        string,
  page_view_attribute_48        string,
  page_view_attribute_49        string,
  page_view_attribute_50        string,
  search_result_count           decimal(8,0)
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_PAGE_VIEW}'
tblproperties ("skip.header.line.count"="1");


--# incoming_product_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_PRODUCT_VIEW}
(
  session_id                     string,
  cookie_id                      string,
  timestamp                      timestamp,
  product_name                   string,
  product_id                     string,
  page_id                        string,
  product_category_id            string,
  product_category               string,
  product_category_top           string,
  product_category_bottom        string,
  site_id                        string,
  avg_star_rating                string,
  number_of_reviews              string,
  brand                          string,
  available_quantity             string,
  product_view_attribute_5       string,
  product_view_attribute_6       string,
  product_view_attribute_7       string,
  product_view_attribute_8       string,
  product_view_attribute_9       string,
  effort_code                    string,
  site_promotion_tag             string,
  availability_of_gallery_images string,
  discontinued_item              string,
  product_view_attribute_14      string,
  product_view_attribute_15      string,
  tea_leaf_jsessionid            string,
  style_id                       string,
  style_description              string,
  product_view_attribute_19      string,
  product_view_attribute_20      string,
  product_view_attribute_21      string, 
  product_view_attribute_22      string,
  product_view_attribute_23      string,
  product_view_attribute_24      string,
  product_view_attribute_25      string,
  product_view_attribute_26      string,
  product_view_attribute_27      string,
  product_view_attribute_28      string,
  product_view_attribute_29      string,
  product_view_attribute_30      string,
  product_view_attribute_31      string,
  product_view_attribute_32      string,
  product_view_attribute_33      string,
  product_view_attribute_34      string,
  product_view_attribute_35      string,
  product_view_attribute_36      string,
  product_view_attribute_37      string,
  product_view_attribute_38      string,
  product_view_attribute_39      string,
  product_view_attribute_40      string,
  product_view_attribute_41      string,
  product_view_attribute_42      string,
  product_view_attribute_43      string,
  product_view_attribute_44      string,
  product_view_attribute_45      string,
  product_view_attribute_46      string,
  product_view_attribute_47      string,
  product_view_attribute_48      string,
  product_view_attribute_49      string,
  product_view_attribute_50      string
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_PRODUCT_VIEW}'
tblproperties ("skip.header.line.count"="1");


--# incoming_real_estate_click

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_REAL_ESTATE}
(
  session_id            string,
  cookie_id             string,
  timestamp             timestamp,
  real_estate_page      string,
  real_estate_version   string,
  real_estate_page_area string,
  real_estate_link      string,
  site_id               string

)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_REAL_ESTATE}'
tblproperties ("skip.header.line.count"="1");


--# incoming_registration

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_REGISTRATION}
(
  session_id                  string,
  cookie_id                   string,
  registration_id             string,
  new_repeat_buyer            string,
  new_repeat_visitor          string,
  visitor_customer_flag       string,
  registered_city             string,
  registered_state            string,
  registered_country          string,
  registered_zip_code         string,
  gender                      string,
  email_address               string,
  age                         string,
  reg1                        string,
  reg2                        string,
  reg3                        string,
  reg4                        string,
  reg5                        string,
  reg6                        string, 
  reg7                        string,
  reg8                        string,
  reg9                        string,
  reg10                       string,
  registration_attribute_11   string,
  account_type_name           string,
  customer_type               string, 
  customer_type_description   string,
  registration_15_test        string, 
  user_store_id               string,
  customer_id                 string,
  customer_first_name         string,
  customer_last_name          string,
  company_name                string,
  omxodp_acccount_merger_type string,
  coupon_code                 string, 
  loyaltyid                   string,
  loyaltyid_flag              string,
  registration_attribute_25   string,
  registration_attribute_26   string,
  registration_attribute_27   string,
  registration_attribute_28   string, 
  registration_attribute_29   string,
  registration_attribute_30   string,
  registration_attribute_31   string,
  registration_attribute_32   string,
  registration_attribute_33   string,
  registration_attribute_34   string,
  registration_attribute_35   string,
  registration_attribute_36   string,
  registration_attribute_37   string,
  registration_attribute_38   string,
  registration_attribute_39   string, 
  registration_attribute_40   string,
  registration_attribute_41   string,
  registration_attribute_42   string,
  registration_attribute_43   string, 
  registration_attribute_44   string,
  registration_attribute_45   string,
  registration_attribute_46   string,
  registration_attribute_47   string, 
  registration_attribute_48   string,
  registration_attribute_49   string,
  registration_attribute_50   string,
  last_update_date            timestamp 
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_REGISTRATION}'
tblproperties ("skip.header.line.count"="1");


--# incoming_session_first_page_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_SESSION_FIRST}
(
  session_id               string,
  cookie_id                string,
  first_timestamp          timestamp,
  first_referring_url      string,
  first_destination_url    string,
  first_referral_type      string,
  ip_address               string,
  timestamp                timestamp,
  referral_name            string,
  referring_url            string,
  referral_type            string,
  natural_search_term      string,
  destination_url          string,
  user_agent               string,
  search_engine            string,
  marketing_vendor         string,
  marketing_category       string,
  marketing_placement      string,
  marketing_item           string,
  visitor_ad_impression_id decimal(30,0)
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_SESSION_FIRST}'
tblproperties ("skip.header.line.count"="1");


--# incoming_site_promotion_click

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_SITE_PROMOTION}
(
  session_id           string,
  cookie_id            string,
  timestamp            timestamp,
  site_promotion_type  string,
  site_promotion_promo string,
  site_promotion_link  string, 
  site_promotion_page  string,
  site_id              string 
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_SITE_PROMOTION}'
tblproperties ("skip.header.line.count"="1");


--# incoming_technical_properties

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_TECHNICAL_PROPERTIES}
(
  session_id            string,
  cookie_id             string,
  timestamp             timestamp,
  browser_type          string,
  javascript_version    string,
  language              string,
  screen_resolution     string,
  color_depth           string,
  operating_system      string,
  time_zone             string,
  mobile_network        string,
  connection_type       string,
  connection_speed      string,
  is_mobile_device      string,
  mobile_device         string,
  device_marketing_name string,
  device_model          string,
  device_type           string,
  device_vendor         string,
  cookie_support        string,
  flash_support         string,
  touch_screen          string,
  video_3gp_support     string,
  video_mp4_support     string,
  video_wmv_support     string,
  java_enabled          string
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_TECHNICAL_PROPERTIES}'
tblproperties ("skip.header.line.count"="1");


--# incoming_validation

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_VALIDATION}
(
	raw_file_name		string,
	record_count	   bigint
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_VALIDATION}';


--# gold_cart_item_abandonment

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_ABANDONMENT}
(
  session_id               string,
  cookie_id                string,
  time_stamp                timestamp,
  product_id               string,
  product_name             string,
  product_category         string,
  product_category_id      string,
  product_category_top     string,
  product_category_bottom  string,
  base_price               float,
  quantity                 string,
  order_id                 string,
  site_id                  string,
  delivery_mode            string,
  avg_rating               string,
  total_review_count       string,
  brand_code               string,
  product_avaliability     string,
  promo_type               string,
  shipping_description     string,
  abandonment_attribute_8  string,
  abandonment_attribute_9  string,
  b2b_b2c                  string,
  abandonment_attribute_11 string,
  effort_code              string,
  discount_total           string,
  abandonment_attribute_14 string,
  atc_source               string,
  list_source              string,
  ssc_list_grade           string,
  atc_location             string,
  atc_referral_pid         string,
  atc_referral_url         string,
  shop_21                  string,
  style_id                 string,
  style_description        string,
  abandonment_attribute_24 string,
  abandonment_attribute_25 string,
  abandonment_attribute_26 string, 
  abandonment_attribute_27 string,
  abandonment_attribute_28 string,
  abandonment_attribute_29 string,
  abandonment_attribute_30 string,
  abandonment_attribute_31 string,
  abandonment_attribute_32 string,
  abandonment_attribute_33 string,
  abandonment_attribute_34 string,
  abandonment_attribute_35 string,
  abandonment_attribute_36 string,
  abandonment_attribute_37 string,
  abandonment_attribute_38 string,
  abandonment_attribute_39 string,
  abandonment_attribute_40 string,
  abandonment_attribute_41 string,
  abandonment_attribute_42 string,
  abandonment_attribute_43 string,
  abandonment_attribute_44 string,
  abandonment_attribute_45 string,
  abandonment_attribute_46 string,
  abandonment_attribute_47 string,
  abandonment_attribute_48 string,
  abandonment_attribute_49 string,
  abandonment_attribute_50 string,
  ip_address               string,
  city                     string,
  state                    string,
  country                  string,
  dma                      string,
  second_level_domain      string,
  client_name 			   string,
  country_code             string,
  language 				   string,
  country_description              string,
  visitor_segment          string
  )
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_ABANDONMENT}';



--# gold_cart_item_addition

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_ADDITION}
(
  session_id              string,
  cookie_id               string,
  time_stamp               timestamp,
  product_id              string,
  product_name            string,
  product_category        string,
  product_category_id     string,
  product_category_top    string,
  product_category_bottom string,
  base_price              float,
  quantity                string,
  order_id                string,
  site_id                 string,
  delivery_mode           string,
  avg_rating              string,
  total_review_count      string,
  brand_code              string,
  product_avaliability    string,
  promo_type              string,
  shipping_description    string,
  cart_attribute_8        string,
  cart_attribute_9        string,
  b2b_b2c                 string,
  cart_attribute_11       string,
  effort_code             string,
  discount_total          string,
  cart_attribute_14       string,
  atc_source              string,
  list_source             string,
  ssc_list_grade          string,
  atc_location            string,
  atc_referral_pid        string,
  atc_referral_url        string,
  shop_21                 string,
  style_id                string,
  style_description       string, 
  cart_attribute_24       string,
  cart_attribute_25       string,
  cart_attribute_26       string,
  cart_attribute_27       string,
  cart_attribute_28       string,
  cart_attribute_29       string,
  cart_attribute_30       string,
  cart_attribute_31       string,
  cart_attribute_32       string,
  cart_attribute_33       string,
  cart_attribute_34       string,
  cart_attribute_35       string,
  cart_attribute_36       string,
  cart_attribute_37       string,
  cart_attribute_38       string,
  cart_attribute_39       string, 
  cart_attribute_40       string,
  cart_attribute_41       string,
  cart_attribute_42       string,
  cart_attribute_43       string,
  cart_attribute_44       string,
  cart_attribute_45       string,
  cart_attribute_46       string,
  cart_attribute_47       string,
  cart_attribute_48       string,
  cart_attribute_49       string,
  cart_attribute_50       string,
  ip_address              string,
  city                    string,
  state                   string,
  country                 string,
  dma                     string,
  second_level_domain     string,
  client_name 			  string,
  country_code 			  string,
  language 				  string,
  country_description 			  string,
  visitor_segment 		  string
)
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_ADDITION}';



--# gold_cart_item_purchase

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_PURCHASE}
(
  session_id              string,
  cookie_id               string,
  time_stamp               timestamp,
  product_id              string,
  product_name            string,
  product_category        string,
  product_category_id     string,
  product_category_top    string,
  product_category_bottom string,
  base_price              float,
  quantity                string,
  order_id                string,
  site_id                 string,
  delivery_mode           string,
  avg_rating              string,
  total_review_count      string,
  brand_code              string,  
  product_avaliability    string,
  promo_type              string,
  shipping_description    string,
  purchase_attribute_8    string,  
  purchase_attribute_9    string,
  b2b_b2c                 string,
  purchase_attribute_11   string,
  effort_code             string,
  discount_total          string,
  purchase_attribute_14   string,
  atc_source              string, 
  list_source             string,
  ssc_list_grade          string,
  atc_location            string,
  atc_referral_pid        string,
  atc_referral_url        string,
  shop_21                 string,
  style_id                string,
  style_description       string,
  purchase_attribute_24   string,
  purchase_attribute_25   string, 
  purchase_attribute_26   string,
  purchase_attribute_27   string,
  purchase_attribute_28   string,
  purchase_attribute_29   string, 
  purchase_attribute_30   string,
  purchase_attribute_31   string,
  purchase_attribute_32   string,
  purchase_attribute_33   string,
  purchase_attribute_34   string,
  purchase_attribute_35   string, 
  purchase_attribute_36   string,
  purchase_attribute_37   string,
  purchase_attribute_38   string,
  purchase_attribute_39   string,
  purchase_attribute_40   string,
  purchase_attribute_41   string,
  purchase_attribute_42   string,
  purchase_attribute_43   string,
  purchase_attribute_44   string,
  purchase_attribute_45   string,
  purchase_attribute_46   string,
  purchase_attribute_47   string,
  purchase_attribute_48   string,
  purchase_attribute_49   string,
  purchase_attribute_50   string
   
)
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_PURCHASE}';



--# gold_conversion_event

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_CONVERSION}
(
  session_id                    string,
  cookie_id                     string,
  time_stamp                     timestamp,
  event_name                    string,
  event_category                string,
  event_action_type             string,
  event_points                  string,
  site_id                       string,
  ce_ex1_smart_cart_user_id     string,
  ce_ex2_smart_cart_account     string,
  ce_ex3_smartcartoriginal_sku  string,
  ce_ex4_smart_cart_original_q  string,
  ce_ex5_smart_cart_original_p  string,
  ce_ex6_smartcartpurchased_sku string,
  ce_ex7_smartcartpurchased_q   string,
  ce_ex8_smartcartpurchased_p   string, 
  ce_ex9_smart_cart_savings     string,
  ce_ex10_signup_city           string,
  ce_ex11_signup_state          string,
  ce_ex12_signup_number_of_emp  string,
  ce_ex13_signup_bus_industry   string,
  ce_ex14_signup_title          string, 
  subscription_sku              string,
  conversion_event_attribute_16 string,
  conversion_event_attribute_17 string,
  conversion_event_attribute_18 string,
  conversion_event_attribute_19 string,
  conversion_event_attribute_20 string,
  conversion_event_attribute_21 string,
  conversion_event_attribute_22 string,
  conversion_event_attribute_23 string,
  conversion_event_attribute_24 string, 
  conversion_event_attribute_25 string,
  conversion_event_attribute_26 string,
  conversion_event_attribute_27 string,
  conversion_event_attribute_28 string,
  conversion_event_attribute_29 string,
  conversion_event_attribute_30 string,
  conversion_event_attribute_31 string,
  conversion_event_attribute_32 string,
  conversion_event_attribute_33 string,
  conversion_event_attribute_34 string,
  conversion_event_attribute_35 string, 
  conversion_event_attribute_36 string,
  conversion_event_attribute_37 string,
  conversion_event_attribute_38 string, 
  conversion_event_attribute_39 string,
  conversion_event_attribute_40 string,
  conversion_event_attribute_41 string,
  conversion_event_attribute_42 string,
  conversion_event_attribute_43 string,
  conversion_event_attribute_44 string,
  conversion_event_attribute_45 string,
  conversion_event_attribute_46 string,
  conversion_event_attribute_47 string,
  conversion_event_attribute_48 string,
  conversion_event_attribute_49 string,
  conversion_event_attribute_50 string
  
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_CONVERSION}';




--# gold_element

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_ELEMENT}
(
  session_id                          string,
  cookie_id                           string,
  time_stamp                           timestamp,
  element_name                        string,
  element_category                    string,
  site_id                             string,
  product_page_sku                    string,
  endecca_refinement_category         string,
  endecca_refinement                  string, 
  error_messaging_product             string,
  endecca_refinement_product_category string,
  csl_add_to_list_from_location       string,
  csl_add_to_list_type                string,
  csl_create_list_from                string,
  csl_manage_list_link_from           string,
  csl_paginate_depth_lb_pg_num        string,
  csl_paginate_from                   string,
  csl_set_default_list_from           string,
  csl_sort_list_by                    string,
  csl_sort_list_from                  string,
  grand_total                         string,
  subscription_sku                    string,
  monetate_element                    string, 
  test_exposure                       string,
  local_browse_search_term            string,
  tab_name                            string,
  order_hist_page_srch_item           string,
  order_hist_page_srch_ord_type       string,
  order_hist_pg_srch_ord_status       string,
  order_hist_pg_prod_grp_filter       string,
  order_hist_pg_prod_cat_filter       string,
  order_hist_pg_sub_cat_filter        string,
  video_status                        string,
  video_time_stamp                    string,
  video_length                        string,
  predictive_test_painted_result      string,
  predictive_test_search_term         string,
  element_attribute_32                string,
  element_attribute_33                string,
  element_attribute_34                string,
  element_attribute_35                string,
  element_attribute_36                string,
  element_attribute_37                string,
  element_attribute_38                string,
  element_attribute_39                string,
  element_attribute_40                string,
  element_attribute_41                string,
  element_attribute_42                string,
  element_attribute_43                string,
  element_attribute_44                string,
  element_attribute_45                string,
  element_attribute_46                string,
  element_attribute_47                string, 
  element_attribute_48                string,
  element_attribute_49                string,
  element_attribute_50                string
  
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_ELEMENT}';




--# gold_geography

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_GEOGRAPHY}
(
  session_id          string,
  cookie_id           string,
  ip_address          string,
  city                string,
  state               string,
  country             string,
  dma                 string,
  second_level_domain string
    
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_GEOGRAPHY}';




--# gold_mmc_click

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_MMC_CLICK}
(
  session_id             string,
  cookie_id              string,
  time_stamp              timestamp,
  marketing_vendor       string,
  marketing_category     string,
  marketing_placement    string,
  marketing_item         string,
  site_id                string,
  mmc_click_attribute_1  string,
  mmc_click_attribute_2  string,
  mmc_click_attribute_3  string,
  mmc_click_attribute_4  string,
  mmc_click_attribute_5  string,
  mmc_click_attribute_6  string,
  mmc_click_attribute_7  string,
  mmc_click_attribute_8  string,
  mmc_click_attribute_9  string,
  mmc_click_attribute_10 string,
  mmc_click_attribute_11 string,
  mmc_click_attribute_12 string,
  mmc_click_attribute_13 string,
  mmc_click_attribute_14 string,
  mmc_click_attribute_15 string,
  mmc_click_attribute_16 string,
  mmc_click_attribute_17 string,
  mmc_click_attribute_18 string,
  mmc_click_attribute_19 string,
  mmc_click_attribute_20 string,
  mmc_click_attribute_21 string, 
  mmc_click_attribute_22 string,
  mmc_click_attribute_23 string,
  mmc_click_attribute_24 string,
  mmc_click_attribute_25 string,
  mmc_click_attribute_26 string,
  mmc_click_attribute_27 string,
  mmc_click_attribute_28 string,
  mmc_click_attribute_29 string,
  mmc_click_attribute_30 string,
  mmc_click_attribute_31 string,
  mmc_click_attribute_32 string,
  mmc_click_attribute_33 string,
  mmc_click_attribute_34 string,
  mmc_click_attribute_35 string,
  mmc_click_attribute_36 string,
  mmc_click_attribute_37 string,
  mmc_click_attribute_38 string,
  mmc_click_attribute_39 string,
  mmc_click_attribute_40 string,
  mmc_click_attribute_41 string,
  mmc_click_attribute_42 string,
  mmc_click_attribute_43 string,
  mmc_click_attribute_44 string,
  mmc_click_attribute_45 string,
  mmc_click_attribute_46 string,
  mmc_click_attribute_47 string,
  mmc_click_attribute_48 string,
  mmc_click_attribute_49 string,
  mmc_click_attribute_50 string
   
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_MMC_CLICK}';




--# gold_order

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_ORDER}
(
  session_id               string,
  cookie_id                string,
  time_stamp                timestamp,
  order_id                 string, 
  registration_id          string,
  order_total              float,
  shipping                 float,
  site_id                  string,
  delivery_mode            string,
  account_type             string,
  customer_type_b2b_or_b2c string,
  customer_type            string,
  order_attribute_5        string,
  order_attribute_6        string,
  order_attribute_7        string,
  coupons_total            string,
  association_discount     string,
  unknown_data_field       string,
  tender_type_credit_card  string,
  tender_type_paypal       string,
  tender_type_gift_card    string,
  coupon_code              string,
  loyaltyid                string,
  order_attribute_16       string,
  order_attribute_17       string,
  order_attribute_18       string,
  order_attribute_19       string,
  order_attribute_20       string,
  order_attribute_21       string,
  order_attribute_22       string,
  order_attribute_23       string,
  order_attribute_24       string,
  order_attribute_25       string,
  order_attribute_26       string,
  order_attribute_27       string,
  order_attribute_28       string,
  order_attribute_29       string,
  order_attribute_30       string,
  order_attribute_31       string,
  order_attribute_32       string,
  order_attribute_33       string,
  order_attribute_34       string,
  order_attribute_35       string,
  order_attribute_36       string,
  order_attribute_37       string,
  order_attribute_38       string,
  order_attribute_39       string,
  order_attribute_40       string,
  order_attribute_41       string,
  order_attribute_42       string,
  order_attribute_43       string,
  order_attribute_44       string,
  order_attribute_45       string,
  order_attribute_46       string,
  order_attribute_47       string,
  order_attribute_48       string,
  order_attribute_49       string,
  order_attribute_50       string, 
  ip_address              string,
  city                    string,
  state                   string,
  country                 string,
  dma                     string,
  second_level_domain     string,
  client_name 			  string,
  country_code 			  string,
  language 				  string,
  country_description 			  string,
  visitor_segment 		  string
    
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_ORDER}';




--# gold_page_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_PAGE_VIEW}
(
  session_id                    string,
  cookie_id                     string,
  time_stamp                     timestamp,
  page                          string,
  page_id                       string,
  content_category              string,
  content_category_id           string,
  content_category_top          string,
  content_category_bottom       string,
  on_site_search_term           string,
  page_url                      string,
  page_referral_url             string,
  site_id                       string,
  browse_type_search_type       string,
  list_grid_view                string,
  sort_state                    string,
  sort_size                     string,
  app_version                   string,
  app_device_name               string,
  app_os                        string,
  b2b_b2c                       string,
  ink_and_toner_search          string,
  onsite_search_term            string,
  refinement_selection          string,
  search_refinement_category    string,
  search_refinement_item        string,
  search_refinement_combination string,
  original_search_term          string,
  tea_leaf_jsessionid           string,
  related_search_terms          string,
  account_source                string,
  customerid                    string,
  account_type                  string,
  page_view_attribute_21        string,
  page_view_attribute_22        string,
  page_view_attribute_23        string,
  page_view_attribute_24        string,
  page_view_attribute_25        string,
  page_view_attribute_26        string,
  page_view_attribute_27        string,
  page_view_attribute_28        string,
  page_view_attribute_29        string,
  page_view_attribute_30        string,
  page_view_attribute_31        string,
  page_view_attribute_32        string,
  page_view_attribute_33        string,
  page_view_attribute_34        string,
  page_view_attribute_35        string,
  page_view_attribute_36        string,
  page_view_attribute_37        string,
  page_view_attribute_38        string,
  page_view_attribute_39        string,
  page_view_attribute_40        string,
  page_view_attribute_41        string,
  page_view_attribute_42        string,
  page_view_attribute_43        string,
  page_view_attribute_44        string,
  page_view_attribute_45        string,
  page_view_attribute_46        string,
  page_view_attribute_47        string,
  page_view_attribute_48        string,
  page_view_attribute_49        string,
  page_view_attribute_50        string,
  search_result_count           string,
  ip_address              		string,
  city                    		string,
  state                   		string,
  country               	    string,
  dma                     		string,
  second_level_domain     		string,
  client_name 			  		string,
  country_code 			  		string,
  language 				  		string,
  country_description 			  		string,
  visitor_segment 		  		string 
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_PAGE_VIEW}';




--# gold_product_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_PRODUCT_VIEW}
(
  session_id                     string,
  cookie_id                      string,
  time_stamp                      timestamp,
  product_name                   string,
  product_id                     string,
  page_id                        string,
  product_category_id            string,
  product_category               string,
  product_category_top           string,
  product_category_bottom        string,
  site_id                        string,
  avg_star_rating                string,
  number_of_reviews              string,
  brand                          string,
  available_quantity             string,
  product_view_attribute_5       string,
  product_view_attribute_6       string,
  product_view_attribute_7       string,
  product_view_attribute_8       string,
  product_view_attribute_9       string,
  effort_code                    string,
  site_promotion_tag             string,
  availability_of_gallery_images string,
  discontinued_item              string,
  product_view_attribute_14      string,
  product_view_attribute_15      string,
  tea_leaf_jsessionid            string,
  style_id                       string,
  style_description              string,
  product_view_attribute_19      string,
  product_view_attribute_20      string,
  product_view_attribute_21      string, 
  product_view_attribute_22      string,
  product_view_attribute_23      string,
  product_view_attribute_24      string,
  product_view_attribute_25      string,
  product_view_attribute_26      string,
  product_view_attribute_27      string,
  product_view_attribute_28      string,
  product_view_attribute_29      string,
  product_view_attribute_30      string,
  product_view_attribute_31      string,
  product_view_attribute_32      string,
  product_view_attribute_33      string,
  product_view_attribute_34      string,
  product_view_attribute_35      string,
  product_view_attribute_36      string,
  product_view_attribute_37      string,
  product_view_attribute_38      string,
  product_view_attribute_39      string,
  product_view_attribute_40      string,
  product_view_attribute_41      string,
  product_view_attribute_42      string,
  product_view_attribute_43      string,
  product_view_attribute_44      string,
  product_view_attribute_45      string,
  product_view_attribute_46      string,
  product_view_attribute_47      string,
  product_view_attribute_48      string,
  product_view_attribute_49      string,
  product_view_attribute_50      string,  
 ip_address           		     string,
  city                           string,
  state                   		string,
  country                 		string,
  dma                     		string,
  second_level_domain     		string,
  client_name 			  		string,
  country_code 			  		string,
  language 				  		string,
  country_description 			  		string,
  visitor_segment 		  		string    
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_PRODUCT_VIEW}';



--# gold_real_estate_click

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_REAL_ESTATE}
(
  session_id            string,
  cookie_id             string,
  time_stamp             timestamp,
  real_estate_page      string,
  real_estate_version   string,
  real_estate_page_area string,
  real_estate_link      string,
  site_id               string
   
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_REAL_ESTATE}';




--# gold_registration

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_REGISTRATION}
(
  session_id                  string,
  cookie_id                   string,
  new_repeat_buyer            string,
  new_repeat_visitor          string,
  visitor_customer_flag       string,
  registered_city             string,
  registered_state            string,
  registered_country          string,
  registered_zip_code         string,
  gender                      string,
  age                         string,
  reg1                        string,
  reg2                        string,
  reg3                        string,
  reg4                        string,
  reg5                        string,
  reg6                        string, 
  reg7                        string,
  reg8                        string,
  reg9                        string,
  reg10                       string,
  registration_attribute_11   string,
  account_type_name           string,
  customer_type               string, 
  customer_type_description   string,
  registration_15_test        string, 
  user_store_id               string,
  customer_id                 string,
  customer_first_name         string,
  customer_last_name          string,
  company_name                string,
  omxodp_acccount_merger_type string,
  coupon_code                 string, 
  loyaltyid                   string,
  loyaltyid_flag              string,
  registration_attribute_25   string,
  registration_attribute_26   string,
  registration_attribute_27   string,
  registration_attribute_28   string, 
  registration_attribute_29   string,
  registration_attribute_30   string,
  registration_attribute_31   string,
  registration_attribute_32   string,
  registration_attribute_33   string,
  registration_attribute_34   string,
  registration_attribute_35   string,
  registration_attribute_36   string,
  registration_attribute_37   string,
  registration_attribute_38   string,
  registration_attribute_39   string, 
  registration_attribute_40   string,
  registration_attribute_41   string,
  registration_attribute_42   string,
  registration_attribute_43   string, 
  registration_attribute_44   string,
  registration_attribute_45   string,
  registration_attribute_46   string,
  registration_attribute_47   string, 
  registration_attribute_48   string,
  registration_attribute_49   string,
  registration_attribute_50   string,
  last_update_date            string
  
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_REGISTRATION}';



--# gold_session_first_page_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_SESSION_FIRST}
(
  session_id               string,
  cookie_id                string,
  first_time_stamp          timestamp,
  first_referring_url      string,
  first_destination_url    string,
  first_referral_type      string,
  ip_address               string,
  time_stamp                timestamp,
  referral_name            string,
  referring_url            string,
  referral_type            string,
  natural_search_term      string,
  destination_url          string,
  user_agent               string,
  search_engine            string,
  marketing_vendor         string,
  marketing_category       string,
  marketing_placement      string,
  marketing_item           string,
  visitor_ad_impression_id string
  
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_SESSION_FIRST}';




--# gold_site_promotion_click

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_SITE_PROMOTION}
(
  session_id           string,
  cookie_id            string,
  time_stamp            timestamp,
  site_promotion_type  string,
  site_promotion_promo string,
  site_promotion_link  string, 
  site_promotion_page  string,
  site_id              string
  
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_SITE_PROMOTION}';




--# gold_technical_properties

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_TECHNICAL_PROPERTIES}
(
  session_id            string,
  cookie_id             string,
  time_stamp             timestamp,
  browser_type          string,
  javascript_version    string,
  language              string,
  screen_resolution     string,
  color_depth           string,
  operating_system      string,
  time_zone             string,
  mobile_network        string,
  connection_type       string,
  connection_speed      string,
  is_mobile_device      string,
  mobile_device         string,
  device_marketing_name string,
  device_model          string,
  device_type           string,
  device_vendor         string,
  cookie_support        string,
  flash_support         string,
  touch_screen          string,
  video_3gp_support     string,
  video_mp4_support     string,
  video_wmv_support     string,
  java_enabled          string
   
)  
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_TECHNICAL_PROPERTIES}';





--# gold_validation

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_VALIDATION}
(
	raw_file_name		string,
	record_count	   bigint
	
)
PARTITIONED BY (batch_id string)
--STORED AS PARQUET
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_VALIDATION}';




--# work_geography_country_cookie

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_GEOGRAPHY_COUNTRY_COOKIE}
(
	session_id          string,
    cookie_id           string,
    visitor_segment     string, 
    ip_address          string,
    city                string,
    state               string,
    country             string,
    second_level_domain string,
    client_name         string,
    country_code        string,
    language            string,
    country_description         string,
    dma                 string
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_GEOGRAPHY_COUNTRY_COOKIE}';

--# work_cart_item_abandonemnt

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_ABANDONMENT}
(
  country_description               string,
  visitor_segment		string,
  total_cart_abandonment   bigint
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_ABANDONMENT}';


--# work_cart_item_addition

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_ADDITION}
(
  country_description                       string,
  visitor_segment			    string,
  total_cart_started            bigint
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_ADDITION}';

--# work_order

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_ORDER}
(
  country_description                                       string,
  visitor_segment									string,
  total_orders                                   bigint,
  total_revenue_from_new_customer               double,
  total_revenue_from_existing_customer          double,
  total_revenue_from_anonymous_customer         double,
  load_date                     string
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_ORDER}';

--##############################################################################

--# work_page_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_PAGE_VIEW}
(
  country_description                       string,
  visitor_segment				string,
  total_page_view               bigint,
  unique_session_id             bigint,
  load_date                     string
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_PAGE_VIEW}';

--##############################################################################

--# work_product_view

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_PRODUCT_VIEW}
(
  country_description                                       string,
  visitor_segment								string,
  total_customers_who_viewed_product            bigint
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_PRODUCT_VIEW}';

--##############################################################################

--# job_statistics #

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_AUDIT}.${hiveconf:TBL_JOB_STATISTIC}
(
  batchId                                       string,
  job_name                                       string,
  table_name                                     string,
  user                                          string,
  start_time                                     string,
  end_time                                       string,
  total_time                                     string,
  no_of_records                                   string,
  status                                        string,
  record_type                                    string,
  log_path                                       string
)
PARTITIONED BY (current_date string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '${hiveconf:AUDITLOG_HDFS}/${hiveconf:TBL_JOB_STATISTIC}';

--# validation
  
  CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_AUDIT}.${hiveconf:TBL_VALIDATION}
(
	  incoming_total_orders              int,
	  incoming_total_visits              int,
	  incoming_total_page_views          int,
	  incoming_total_revenue             decimal(30,10),
	  incoming_total_cart_started        int,
	  incoming_total_cart_abandonment	 int,
	  incoming_total_viewed_products	 int,
	  hbase_total_orders                 int,
	  hbase_total_visits                 int,
	  hbase_total_page_views             int,
	  habse_total_revenue                decimal(30,10),
	  hbase_total_cart_started           int,
	  hbase_total_cart_abandonment	     int,
	  hbase_total_viewed_products	     int
	)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LOCATION '${hiveconf:AUDITLOG_HDFS}/${hiveconf:TBL_VALIDATION}';


--##############################################################################
--#                                    End                                     #
--##############################################################################
