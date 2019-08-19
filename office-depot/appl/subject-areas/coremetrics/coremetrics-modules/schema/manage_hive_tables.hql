--#######################################################################################
--#                              General Details                               		    #
--#######################################################################################
--#                                                                            		    #
--# Name                                                                                #
--#     : manage_hive_tables      								   	 		            #
--# File                                                                                #
--#     : manage_hive_tables.hql                                   	                    #
--# Description                                                                         #
--#     : Contains ddl for Hive Schema 		  									        #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--# Author                                                                              #
--#     : Deepanshu and Shweta                			 					            #
--#                                                                                     #
--#######################################################################################


--# gold_cart_item_abandonment
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_ABANDONMENT} add partition (batch_id=${hiveconf:batchId});


--# gold_cart_item_addition
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_ADDITION} add partition (batch_id=${hiveconf:batchId});


--# gold_cart_item_purchase
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_PURCHASE} add partition (batch_id=${hiveconf:batchId});


--# gold_conversion_event
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_CONVERSION} add partition (batch_id=${hiveconf:batchId});


--# gold_element
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_ELEMENT} add partition (batch_id=${hiveconf:batchId});


--# gold_geography
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_GEOGRAPHY} add partition (batch_id=${hiveconf:batchId});


--# gold_mmc_click
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_MMC_CLICK} add partition (batch_id=${hiveconf:batchId});


--# gold_order
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_ORDER} add partition (batch_id=${hiveconf:batchId});


--# gold_page_view
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_PAGE_VIEW} add partition (batch_id=${hiveconf:batchId});


--# gold_product_view
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_PRODUCT_VIEW} add partition (batch_id=${hiveconf:batchId});


--# gold_real_estate_click
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_REAL_ESTATE} add partition (batch_id=${hiveconf:batchId});


--# gold_registration
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_REGISTRATION} add partition (batch_id=${hiveconf:batchId});


--# gold_session_first_page_view
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_SESSION_FIRST} add partition (batch_id=${hiveconf:batchId});


--# gold_site_promotion_click
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_SITE_PROMOTION} add partition (batch_id=${hiveconf:batchId});


--# gold_technical_properties
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_TECHNICAL_PROPERTIES} add partition (batch_id=${hiveconf:batchId});


--# gold_validation
use ${hiveconf:DB_GOLD}; alter table ${hiveconf:TBL_GOLD_VALIDATION} add partition (batch_id=${hiveconf:batchId});

--##############################################################################
--#                                    End                                     #
--##############################################################################
