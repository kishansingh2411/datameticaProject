--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cdmprddta_dm_customer_account                                      #
--# File                                                                       #
--#     : work_cdmprddta_dm_customer_account.pig                                  #
--# Description                                                                #
--#     : To load data into work layer                                     #
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


SET job.name '$WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT_SSH_ACTION';

work_cdmprddta_dm_customer_account = 
	LOAD '$DB_INCOMING.$INCOMING_CDMPRDDTA_DM_CUSTOMER_ACCOUNT'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_cdmprddta_dm_customer_account_generate = 
	FOREACH work_cdmprddta_dm_customer_account GENERATE
	RTRIM(customer_id),
	RTRIM(country_cd),
	RTRIM(business_unit_type_cd),
	RTRIM(ab_comment_txt),
	RTRIM(account_established_dt),
	RTRIM(account_status_cd),
	RTRIM(account_type_cd),
	RTRIM(allow_backorder_ind),
	RTRIM(allow_split_order_ind),
	RTRIM(allow_substitute_ind),
	RTRIM(furniture_delivery_fee_ind),
	RTRIM(need_credit_approval_ind),
	RTRIM(need_return_approval_ind),
	RTRIM(od_customer_type_cd),
	RTRIM(customer_type_cd),
	RTRIM(location_id),
	RTRIM(customer_nm),
	RTRIM(set_up_channel_id),
	RTRIM(first_order_method_cd),
	RTRIM(first_order_dt),
	RTRIM(wlr_customer_ind),
	RTRIM(premier_dt),
	RTRIM(language_cd),
	RTRIM(tam_agent_id),
	RTRIM(current_credit_status_cd),
	RTRIM(spend_potential_amt),
	RTRIM(share_of_wallet_amt),
	RTRIM(company_size_qty),
	RTRIM(mailable_ind),
	RTRIM(avg_days_to_pay_qty),
	RTRIM(current_balance_amt),
	RTRIM(first_order_effort_id),
	RTRIM(first_order_effort_yr),
	RTRIM(first_order_premium_ind),
	RTRIM(non_rental_ind),
	RTRIM(polybag_ind),
	RTRIM(merge_id),
	RTRIM(prospect_id),
	RTRIM(credit_cd),
	RTRIM(mail_key_cd),
	RTRIM(one_person_company_ind),
	RTRIM(primary_segment_cd),
	RTRIM(mgm_ind),
	RTRIM(first_order_ship_dt),
	RTRIM(thirty_day_ind),
	RTRIM(thirty_day_amt),
	RTRIM(sixty_day_ind),
	RTRIM(sixty_day_amt),
	RTRIM(ninety_day_ind),
	RTRIM(ninety_day_amt),
	RTRIM(tam_target_grp_nm),
	RTRIM(tam_customer_ind),
	RTRIM(phone_ind),
	RTRIM(fax_ind),
	RTRIM(email_ind),
	RTRIM(customer_abcd_class_cd),
	RTRIM(premier_customer_cd),
	RTRIM(standard_industry_cd),
	RTRIM(branch_cd),
	RTRIM(no_of_buyers_cnt),
	RTRIM(no_of_mailable_buyers_cnt),
	RTRIM(work_life_reward_dt),
	RTRIM(customer_source_cd),
	RTRIM(source_ref_tam_agent_id),
	RTRIM(crm_id),
	RTRIM(corporate_cust_account_id),
	RTRIM(consolidated_channel_id),
	RTRIM(phone_num),
	RTRIM(postal_cd),
	RTRIM(payment_terms),
	RTRIM(loyalty_program),
	RTRIM(first_ord_value_amt),
	RTRIM(first_ord_cog_amt),
	RTRIM(first_ord_premium_costs_amt),
	RTRIM(internet_setup_ind),
	RTRIM(last_order_dt),
	RTRIM(last_mail_dt),
	RTRIM(last_effort_mailed),
	RTRIM(loc_sic_cd),
	RTRIM(update_dt),
	RTRIM(ika_id),
	RTRIM(duns_id),
	RTRIM(nat_id),
	RTRIM(ind_type),
	RTRIM(sales_ltv_active_month_amt),
	RTRIM(ltv_sales_amt),
	RTRIM(ltv_orders_cnt),
	RTRIM(ltv_cog_amt),
	RTRIM(ltv_premium_cost_amt),
	RTRIM(org_name2_txt),
	RTRIM(org_name3_txt),
	RTRIM(org_name4_txt),
	RTRIM(vat_number),
	RTRIM(acc_group_cd),
	RTRIM(currency_cd),
	RTRIM(delete_flag),
	RTRIM(current_record_ind),
	RTRIM(mergepurge_dt),
	RTRIM(first_invoice_dt),
	RTRIM(first_invoice_num),
	RTRIM(processed_dt),
	RTRIM(first_order_num),
	RTRIM(spend_band),
	RTRIM(ultimate_parent_id);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_cdmprddta_dm_customer_account_generate 
	INTO '$WORK_HDFS/$WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT/batch_id=$batch_id/'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################

         