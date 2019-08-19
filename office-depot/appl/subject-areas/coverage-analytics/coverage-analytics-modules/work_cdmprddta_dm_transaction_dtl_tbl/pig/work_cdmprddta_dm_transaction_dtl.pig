--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cdmprddta_dm_transaction_dtl                                    #
--# File                                                                       #
--#     : work_cdmprddta_dm_transaction_dtl.pig                                #
--# Description                                                                #
--#     : To load data into work layer                                         #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Sonali Rawool                                                         #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################
Register $OPENCSV_JAR;

SET job.name '$WORK_CDMPRDDTA_DM_TRANSACTION_DTL_SSH_ACTION';

work_cdmprddta_dm_transaction_dtl = 
	LOAD '$DB_INCOMING.$INCOMING_CDMPRDDTA_DM_TRANSACTION_DTL'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_cdmprddta_dm_transaction_dtl_generate = 
	FOREACH work_cdmprddta_dm_transaction_dtl GENERATE 
    RTRIM(transaction_id),
	RTRIM(fulfillment_id),
	RTRIM(accounting_dt),
	RTRIM(association_id),
	RTRIM(bill_to_address_id),
	RTRIM(business_channel_id),
	RTRIM(od_customer_type_cd),
	RTRIM(contract_associate_id),
	RTRIM(country_cd),
	RTRIM(currency_cd),
	RTRIM(customer_id),
	RTRIM(delivery_method_cd),
	RTRIM(inventory_location_id),
	RTRIM(lob_id),
	RTRIM(order_associate_id),
	RTRIM(order_dt),
	RTRIM(order_location_id),
	RTRIM(order_num),
	RTRIM(order_status_cd),
	RTRIM(order_taxable_cd),
	RTRIM(order_type_cd),
	RTRIM(placement_method_cd),
	RTRIM(promised_fulfillment_dt),
	RTRIM(sales_location_id),
	RTRIM(ship_to_address_id),
	RTRIM(ship_to_contact_id),
	RTRIM(winning_effort_id),
	RTRIM(winning_effort_yr),
	RTRIM(order_service_type_cd),
	RTRIM(new_existing_customer_ind),
	RTRIM(invoice_num),
	RTRIM(processed_dt),
	RTRIM(invoice_dt),
	RTRIM(vendor_id),
	RTRIM(unit_selling_price_amt),
	RTRIM(unit_original_price_amt),
	RTRIM(unit_list_price_amt),
	RTRIM(unit_item_cost_amt),
	RTRIM(transaction_line_type),
	RTRIM(transaction_line_num),
	RTRIM(sku_num),
	RTRIM(shipped_qty),
	RTRIM(selling_sku),
	RTRIM(sales_associate_id),
	RTRIM(return_reason_cd),
	RTRIM(price_prefix_cd),
	RTRIM(price_change_rsn_cd),
	RTRIM(price_cd),
	RTRIM(premium_cost_amt),
	RTRIM(order_qty),
	RTRIM(order_line_id),
	RTRIM(order_header_id),
	RTRIM(item_taxable_cd),
	RTRIM(inv_location_id),
	RTRIM(how_priced_cd),
	RTRIM(ext_selling_price_amt),
	RTRIM(ext_item_cost_amt),
	RTRIM(ext_gross_sales_amt),
	RTRIM(ext_gross_cost_amt),
	RTRIM(ext_big_deal_cost_amt),
	RTRIM(entered_sku),
	RTRIM(effort_yr),
	RTRIM(effort_id),
	RTRIM(discount_amt),
	RTRIM(direct_delivery_cd),
	RTRIM(core_ind),
	RTRIM(contract_type_cd),
	RTRIM(contract_seq),
	RTRIM(contract_id),
	RTRIM(commissionable_ind),
	RTRIM(bsd_user),
	RTRIM(bsd_cost_centre),
	RTRIM(big_deal_cost_amt),
	RTRIM(backorder_qty),
	RTRIM(gross_profit);
	  
--##############################################################################
--#                                   Store                                    #
--##############################################################################

  STORE work_cdmprddta_dm_transaction_dtl_generate 
  INTO '$WORK_HDFS/$WORK_CDMPRDDTA_DM_TRANSACTION_DTL/batch_id=$batch_id/'
  USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################