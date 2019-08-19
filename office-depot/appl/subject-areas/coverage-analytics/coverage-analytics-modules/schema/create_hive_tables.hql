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
--#     : Shweta                		            	 					            #
--#                                                                                     #
--#######################################################################################

--Creating databases

set mapred.job.name=${hiveconf:JOB_CREATE_SCHEMA_SSH_ACTION};
add jar ${hiveconf:OPENCSV_JAR};
CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_INCOMING};

CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_GOLD};

CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_AUDIT};
 
CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_WORK};

--# Creating incoming layer tables 

--# Creating incoming_dim_country table

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_DIM_NACE}
(
	code        string,
	section     string,
	description string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_DIM_NACE}';

--# incoming_nl_univ_vikbase

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_NL_UNIV_VIKBASE}
(
	code string,
	vnum string,
	rnum string,
	cham string,
	naam string,
	nama string,
	sstr string,
	huis string,
	hstv string,
	post string,
	plac string,
	pstr string,
	pbus string,
	posp string,
	plap string,
	prov string,
	hbrn string,
	tbrn string,
	beco string,
	bgrt string,
	revo string,
	hffi string,
	teln string,
	tela string,
	faxn string,
	faxa string,
	grow string,
	soho string,
	recl string,
	rere string,
	ecac string,
	novi string,
	fdat string,
	inar string,
	gemc string,
	eggc string,
	coro string,
	cebu string,
	niel string,
	inwo string,
	impc string,
	expc string,
	part string,
	chab string,
	updated string,
	filler string,
	email string,
	wcclass string,
	bvg string,
	nomail string
)
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_NL_UNIV_VIKBASE}';


--# incoming_ge_univ_schober

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_GE_UNIV_SCHOBER}
(
	company_id string,
	name_1 string,
	name_2 string,
	name_3 string,
	country_code_value string,
	zipcode string,
	town string,
	street string,
	po_box string,
	zipcode_po_box string,
	town_po_box string,
	company_salutation string,
	tel_area_code string,
	telephone_no string,
	fax_area_code string,
	fax_no string,
	legal_form string,
	company_size string,
	company_type string,
	year_of_foundation string,
	chamber_of_commerce_no string,
	no_of_employees string,
	turnover string,
	population string,
	capital string,
	homepage string,
	email string,
	mobile_phone_no string,
	service_phone_no string,
	region string,
	mailorder_activity string,
	employee_category string,
	activity_code string,
	no_white_collar_workers string,
	no_blue_collar_workers string
)
PARTITIONED BY (batch_id string, country_code string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_GE_UNIV_SCHOBER}';

--# incoming_it_univ_dbitaly

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_IT_UNIV_DBITALY}
(
	record_id string,
	progressive_record_id string,
	company_name string,
	surname string,
	name string,
	title string,
	address string,
	city_town_description string,
	province_code string,
	postal_code string,
	sex_code string,
	telephone_area_code string,
	telephone_number string,
	movement_type string,
	elaboration_date string,
	activity_group_code string,
	company_registered_status string,
	company_typecode string,
	yellowpages_category_code string,
	yellowpages_description string,
	additional_activity_descr string,
	flag_cotel string,
	flag_copos string,
	flag_primary_secondary string,
	number_of_telephone_lines string,
	region_code_istat string,
	province_code_istat string,
	city_town_code_istat string,
	street_code_seat string,
	viking_terretorial_code string,
	vat_number string,
	activity_date string,
	employee_size_code string,
	employee_size_desc string,
	branch string,
	fiscal_code string,
	scoring_light string,
	changed_town_code string,
	changed_town_description string,
	changed_zip_code string,
	changed_street_code string,
	changed_address string,
	changed_comp_reg string,
	changed_phone_number string,
	changed_phone_lines string,
	changed_yellowpages_code string,
	changed_add_activity_descr string,
	changed_employee_size string,
	changed_scoring_light string,
	changed_sede_principal string,
	changed_vat_number string,
	changed_fiscal_code string,
	telephone_id string,
	registration_date string
)
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_IT_UNIV_DBITALY}';


--# incoming_at_univ_ksv

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_AT_UNIV_KSV}
(
	herold_id string,
	ksv_id string,
	name string,
	street string,
	zipcode string,
	city string,
	no_of_employees string,
	no_white_collar_workers string,
	no_blue_collar_workers string,
	company_name string,
	company_name_ksv string,
	uid_no string,
	telephone_1 string,
	telephone_2 string,
	telephone_3 string,
	telephone_4 string,
	telephone_5 string,
	e_mail_1 string,
	e_mail_2 string,
	e_mail_3 string,
	e_mail_4 string,
	e_mail_5 string,
	cat_branch_group_1 string,
	cat_branch_group_2 string,
	cat_branch_group_3 string,
	cat_branch_group_4 string,
	cat_branch_group_5 string,
	at_nace_branch_no_1 string,
	at_nace_branch_no_2 string,
	at_nace_branch_no_3 string,
	at_nace_branch_no_4 string,
	at_nace_branch_no_5 string,
	at_nace_branch_1 string,
	at_nace_branch_2 string,
	at_nace_branch_3 string,
	at_nace_branch_4 string,
	at_nace_branch_5 string
)
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_AT_UNIV_KSV}';


--# incoming_at_univ_schober

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_AT_UNIV_SCHOBER}
(
	company_id string,
	salutation string,
	title string,
	first_name string,
	last_name string,
	company_name_1 string,
	company_name_2 string,
	company_name_3 string,
	street string,
	street_postalcode string,
	town_po_box string,
	po_box string,
	po_box_postalcode string,
	po_box_city string,
	phone_area_code string,
	phone_number string,
	fax_area_code string,
	fax_number string,
	type_of_company string,
	legal_form string,
	no_of_employees string,
	company_size string,
	no_of_beds string,
	no_chamber_of_commerce string,
	year_foundation string,
	business_capital string,
	turnover string,
	size_of_city string,
	ebc_code string,
	email string,
	homepage string,
	shared_practice string,
	mobilphone string,
	typ_wgs84 string,
	x_geo_wgs84 string,
	y_geo_wgs84 string,
	typ_lambert string,
	x_geo_lambert_konf string,
	y_geo_lambert_konf string,
	nace_2008 string,
	nace_2008_desc string,
	blue_collar_worker string,
	white_collar_worker string,
	growth_indicator string,
	flag_score string
)
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_AT_UNIV_SCHOBER}';


--# incoming_uk_univ_lbmdata

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_UK_UNIV_LBMDATA}
(
	lbm_diallerid string,
	marketdata_urn string,
	company string,
	add1 string,
	add2 string,
	add3 string,
	add4 string,
	town string,
	county string,
	postcode string,
	pcarea string,
	pcdist string,
	postcoderegion string,
	telephone string,
	fax string,
	emps string,
	emps_band string,
	emps_band_large string,
	total_emps string,
	total_emps_band_large string,
	sic5 string,
	sic5_description string,
	sic4 string,
	sic4_description string,
	sic2 string,
	sic2_description string,
	sicsector string,
	sicsector_description string,
	thomson_code string,
	thomson_code_description string,
	lbm5 string,
	lbm5_description string,
	sitetype string,
	no_sites string,
	tps string,
	tpsc string,
	fps string,
	mps string,
	estdate string,
	lbm_businesstype string,
	lbm_businesstype_description string,
	company_verifieddate string,
	companylevel_turnoverband string,
	comp_turnoverband_descr string,
	recency_18mths string,
	recency_12mths string,
	months_recency string,
	siteparentid string,
	lbm_siteparentdiallerid string,
	update_type string,
	last_update_lbm string,
	edw_load_date string,
	edw_amend_date string,
	sic2_2007 string,
	sic2_2007_desc string,
	sic3_2007 string,
	sic3_2007_desc string,
	sic4_2007 string,
	sic4_2007_desc string,
	sic5_2007 string,
	sic5_2007_desc string
 )
PARTITIONED BY (batch_id string, country_code string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_UK_UNIV_LBMDATA}';


--# incoming_ir_univ_billmoss

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_IR_UNIV_BILLMOSS}
(
	siteid float,
	site_name string,
	add1 string,
	add2 string,
	add3 string,
	add4 string,
	add5 string,
	add6 string,
	employees float,
	sic1987_4digit string,
	co_uid float,
	suspend string
 )
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_IR_UNIV_BILLMOSS}';


--# incoming_fr_mdugast_n80fin

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_FR_MDUGAST_N80FIN}
(
	siren string,
	nic string,
	l1_nomen string,
	l2_comp string,
	l3_cadr string,
	l4_voie string,
	l5_disp string,
	l6_post string,
	l7_etrg string,
	zr1 string,
	rpet string,
	depet string,
	arronet string,
	ctonet string,
	comet string,
	libcom string,
	du string,
	tu string,
	uu string,
	codpos string,
	zr2 string,
	tcd string,
	zemet string,
	codevoie string,
	numvoie string,
	indrep string,
	typevoie string,
	libvoie string,
	enseigne string,
	apet700 string,
	apet31 string,
	siege string,
	tefet string,
	efetcent string,
	origine string,
	dcret string,
	mmintret string,
	activnat string,
	lieuact string,
	actisurf string,
	saisonat string,
	modet string,
	dapet string,
	defet string,
	explet string,
	prodpart string,
	auxilt string,
	eaeant string,
	eaeapet string,
	eaesec1t string,
	eaesec2t string,
	nomen string,
	sigle string,
	civilite string,
	cj string,
	tefen string,
	efencent string,
	apen700 string,
	apen31 string,
	aprm string,
	tca string,
	recme string,
	dapen string,
	defen string,
	dcren string,
	mmintren string,
	monoact string,
	moden string,
	explen string,
	eaeann string,
	eaeapen string,
	eaesec1n string,
	eaesec2n string,
	eaesec3n string,
	eaesec4n string,
	nbetexpl string,
	tcaexp string,
	regimp string,
	monoreg string,
	rpen string,
	depcomen string,
	vmaj string,
	vmaj1 string,
	vmaj2 string,
	vmaj3 string,
	siret string,
	maj string,
	dmaj string,
	eve string,
	typetab string,
	tel string,
	dateve string,
	exp_is_actif string
 )
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_FR_MDUGAST_N80FIN}';

--#incoming_be_univ_schober

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_BE_UNIV_SCHOBER}
(
	be_id string,
	be_code string,
	btw_number string,
	company_name string,
	legal_form string,
	street string,
	house_number string,
	house_number_add string,
	postcode string,
	city string,
	legal_district string,
	province string,
	language string,
	nace_bel string,
	main_activity_code string,
	activity_code2 string,
	activity_code3 string,
	activity_code4 string,
	activity_code5 string,
	phone_number string,
	fax_number string,
	employee_size string,
	date_of_foundation string,
	ceo_name string,
	ceo_first_name string,
	ceo_sex string,
	ceo_language string,
	mng_dir_name string,
	mng_dir_first_name string,
	mng_dir_sex string,
	mng_dir_language string,
	internal_schober string,
	mng_pur string,
	mng_pur_first_name string,
	mng_pur_sex string,
	mng_pur_language string,
	mng_hr string,
	mng_hr_first_name string,
	mng_hr_sex string,
	mng_hr_language string,
	mng_it string,
	mng_it_first_name string,
	mng_it_sex string,
	mng_it_language string,
	dir_sales string,
	dir_sales_first_name string,
	dir_sales_sex string,
	dir_sales_language string,
	mng_mkt string,
	mng_mkt_first_name string,
	mng_mkt_sex string,
	mng_mkt_language string,
	dir_financ string,
	dir_financ_first_name string,
	dir_financ_sex string,
	dir_financ_language string,
	email string,
	website string,
	turnover string,
	import1 string,
	export string,
	company_type string,
	employees string,
	white_collar_class string,
	ebi_number string,
	filler string
)
PARTITIONED BY (batch_id  string, country_code string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_BE_UNIV_SCHOBER}';

--# incoming_dm_assigned_customer

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_ASSIGNED_CUSTOMER}
(
	country_cd string,
	business_unit_type_cd string,
	od_customer_type_cd string,
	associate_id string,
	customer_id string,
	start_dt string,
	end_dt string,
	processed_dt string
)
PARTITIONED BY (batch_id  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_ASSIGNED_CUSTOMER}';

--# incoming_dm_transaction_dtl
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_TRANSACTION_DTL}
(
	transaction_id string,
	fulfillment_id int,
	accounting_dt string,
	association_id string,
	bill_to_address_id int,
	business_channel_id string,
	od_customer_type_cd string,
	contract_associate_id string,
	country_cd string,
	currency_cd string,
	customer_id string,
	delivery_method_cd string,
	inventory_location_id int,
	lob_id string,
	order_associate_id string,
	order_dt string,
	order_location_id string,
	order_num string,
	order_status_cd string,
	order_taxable_cd string,
	order_type_cd string,
	placement_method_cd string,
	promised_fulfillment_dt string,
	sales_location_id string,
	ship_to_address_id int,
	ship_to_contact_id int,
	winning_effort_id string,
	winning_effort_yr int,
	order_service_type_cd string,
	new_existing_customer_ind string,
	invoice_num string,
	processed_dt string,
	invoice_dt string,
	vendor_id string,
	unit_selling_price_amt float,
	unit_original_price_amt float,
	unit_list_price_amt float,
	unit_item_cost_amt float,
	transaction_line_type string,
	transaction_line_num int,
	sku_num string,
	shipped_qty int,
	selling_sku string,
	sales_associate_id string,
	return_reason_cd string,
	price_prefix_cd string,
	price_change_rsn_cd string,
	price_cd string,
	premium_cost_amt float,
	order_qty int,
	order_line_id float,
	order_header_id float,
	item_taxable_cd string,
	inv_location_id int,
	how_priced_cd string,
	ext_selling_price_amt float,
	ext_item_cost_amt float,
	ext_gross_sales_amt float,
	ext_gross_cost_amt float,
	ext_big_deal_cost_amt float,
	entered_sku string,
	effort_yr int,
	effort_id string,
	discount_amt float,
	direct_delivery_cd string,
	core_ind string,
	contract_type_cd string,
	contract_seq int,
	contract_id string,
	commissionable_ind string,
	bsd_user string,
	bsd_cost_centre string,
	big_deal_cost_amt float,
	backorder_qty int,
	gross_profit float
)
PARTITIONED BY (batch_id  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_TRANSACTION_DTL}';

--# incoming_dm_cdmprddta_associate

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_ASSOCIATE}
(
	associate_id string,
	business_channel_id string,
	associate_first_name string,
	associate_last_name string,
	job_title_txt string,
	associate_start_dt string,
	associate_end_dt string,
	manager_associate_id string,
	country_cd string,
	associate_status_ind int,
	associate_email string,
	associate_gender_cd string,
	associate_cost_centre string,
	associate_type_cd string,
	associate_peoplesoft_id string,
	associate_position_cd string,
	associate_org_unit string,
	sales_division string,
	sfdc_user_id string,
	sfdc_user_type string,
	sales_team string,
	fte_percentage string,
	active_employee string,
	sfdc_company_channel string,
	managers_sfdc_id string,
	managers_peoplesoft_id string,
	update_id string,
	processed_dt string
)
PARTITIONED BY (batch_id string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_ASSOCIATE}';

--# incoming_dm_cdmprddta_country

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_COUNTRY}
(
	country_cd string,
	country_nm string
)
PARTITIONED BY (batch_id  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_COUNTRY}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_CUSTOMER_ACCOUNT}
(
	customer_id string,
	country_cd string,
	business_unit_type_cd string,
	ab_comment_txt string,
	account_established_dt string,
	account_status_cd string,
	account_type_cd string,
	allow_backorder_ind int,
	allow_split_order_ind int, 
	allow_substitute_ind int,
	furniture_delivery_fee_ind int,
	need_credit_approval_ind int,
	need_return_approval_ind int,
	od_customer_type_cd string,
	customer_type_cd string,
	location_id int,
	customer_nm string, 
	set_up_channel_id string,
	first_order_method_cd string, 
	first_order_dt string,
	wlr_customer_ind int,
	premier_dt string,
	language_cd string,
	tam_agent_id int,
	current_credit_status_cd string,
	spend_potential_amt float,
	share_of_wallet_amt int,
	company_size_qty int,
	mailable_ind int,
	avg_days_to_pay_qty float,
	current_balance_amt float,
	first_order_effort_id string,
	first_order_effort_yr int,
	first_order_premium_ind int,
	non_rental_ind int,
	polybag_ind int,
	merge_id int,
	prospect_id int,
	credit_cd string, 
	mail_key_cd string,
	one_person_company_ind int,
	primary_segment_cd string,
	mgm_ind int,
	first_order_ship_dt string,
	thirty_day_ind int,
	thirty_day_amt float,
	sixty_day_ind int,
	sixty_day_amt float,
	ninety_day_ind int,
	ninety_day_amt float,
	tam_target_grp_nm string,
	tam_customer_ind int,
	phone_ind int,
	fax_ind int,
	email_ind int,
	customer_abcd_class_cd string,
	premier_customer_cd string,
	standard_industry_cd string, 
	branch_cd string,
	no_of_buyers_cnt int,
	no_of_mailable_buyers_cnt int,
	work_life_reward_dt string,
	customer_source_cd string, 
	source_ref_tam_agent_id string,
	crm_id string,
	corporate_cust_account_id string,
	consolidated_channel_id string,
	phone_num string,
	postal_cd string,
	payment_terms string,
	loyalty_program string,
	first_ord_value_amt float,
	first_ord_cog_amt float,
	first_ord_premium_costs_amt float,
	internet_setup_ind int,
	last_order_dt string,
	last_mail_dt string,
	last_effort_mailed string,
	loc_sic_cd string, 
	update_dt string,
	ika_id string,
	duns_id string,
	nat_id string,
	ind_type string, 
	sales_ltv_active_month_amt float,
	ltv_sales_amt float,
	ltv_orders_cnt int, 
	ltv_cog_amt float,
	ltv_premium_cost_amt float,
	org_name2_txt string,
	org_name3_txt string,
	org_name4_txt string, 
	vat_number string,
	acc_group_cd string,
	currency_cd string, 
	delete_flag string,
	current_record_ind int,
	mergepurge_dt string,
	first_invoice_dt string,
	first_invoice_num string,
	processed_dt string,
	first_order_num string,
	spend_band int,
	ultimate_parent_id string
)
PARTITIONED BY (batch_id  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_CUSTOMER_ACCOUNT}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_CALENDAR}
(
	calendar_dt string,
	fiscal_week float,
	fiscal_month float,
	fiscal_quarter float,
	fiscal_half float, 
	fiscal_year float,
	fiscal_period float,
	day string,
	day_number float,
	day_of_year float,
	month_end float,
	fiscal_month_desc string,
	season string,
	previous_day string,
	date_last_week string,
	date_last_month string,
	date_last_year string, 
	date_end_of_previous_period string,
	date_same_weekday_last_year string,
	last_week_end_date string,
	same_period_last_year float,
	day_of_fiscal_month float,
	processed_dt string,
	calendar_month float,
	calendar_year float
)
PARTITIONED BY (batch_id  string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "escapeChar"    = "\""
   )
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_CDMPRDDTA_DM_CALENDAR}';

--############################  WORK Layer #####################################
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_COUNTRY}
(
	country_cd string,
	country_nm string
)
PARTITIONED BY (batch_id  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_COUNTRY}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_CALENDAR}
(
	calendar_dt date,
	fiscal_week decimal(2,0),
	fiscal_month decimal(2,0),
	fiscal_quarter decimal(5,0),
	fiscal_half decimal(5,0), 
	fiscal_year decimal(5,0),
	fiscal_period decimal(6,0),
	day string,
	day_number decimal(5,0),
	day_of_year decimal(3,0),
	month_end decimal(5,0),
	fiscal_month_desc string,
	season string,
	previous_day date,
	date_last_week date,
	date_last_month date,
	date_last_year date, 
	date_end_of_previous_period date,
	date_same_weekday_last_year date,
	last_week_end_date date,
	same_period_last_year decimal(6,0),
	day_of_fiscal_month decimal(2,0),
	processed_dt date,
	calendar_month decimal(2,0),
	calendar_year decimal(5,0)
)
PARTITIONED BY (batch_id  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_CALENDAR}';



CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_TRANSACTION_DTL}
(
	transaction_id string,
	fulfillment_id smallint,
	accounting_dt date,
	association_id string,
	bill_to_address_id int,
	business_channel_id string,
	od_customer_type_cd string,
	contract_associate_id string,
	country_cd string,
	currency_cd string,
	customer_id string,
	delivery_method_cd string,
	inventory_location_id smallint,
	lob_id string,
	order_associate_id string,
	order_dt date,
	order_location_id string,
	order_num string,
	order_status_cd string,
	order_taxable_cd string,
	order_type_cd string,
	placement_method_cd string,
	promised_fulfillment_dt date,
	sales_location_id string,
	ship_to_address_id int,
	ship_to_contact_id int,
	winning_effort_id string,
	winning_effort_yr smallint,
	order_service_type_cd string,
	new_existing_customer_ind string,
	invoice_num string,
	processed_dt date,
	invoice_dt date,
	vendor_id string,
	unit_selling_price_amt decimal(15,2),
	unit_original_price_amt decimal(15,2),
	unit_list_price_amt decimal(15,2),
	unit_item_cost_amt decimal(15,3),
	transaction_line_type string,
	transaction_line_num int,
	sku_num string,
	shipped_qty int,
	selling_sku string,
	sales_associate_id string,
	return_reason_cd string,
	price_prefix_cd string,
	price_change_rsn_cd string,
	price_cd string,
	premium_cost_amt decimal(15,2),
	order_qty int,
	order_line_id decimal(18,0),
	order_header_id decimal(18,0),
	item_taxable_cd string,
	inv_location_id smallint,
	how_priced_cd string,
	ext_selling_price_amt decimal(15,2),
	ext_item_cost_amt decimal(15,3),
	ext_gross_sales_amt decimal(15,2),
	ext_gross_cost_amt decimal(15,3),
	ext_big_deal_cost_amt decimal(15,2),
	entered_sku string,
	effort_yr smallint,
	effort_id string,
	discount_amt decimal(15,2),
	direct_delivery_cd string,
	core_ind string,
	contract_type_cd string,
	contract_seq int,
	contract_id string,
	commissionable_ind string,
	bsd_user string,
	bsd_cost_centre string,
	big_deal_cost_amt decimal(15,2),
	backorder_qty int,
	gross_profit decimal(15,2)
)
PARTITIONED BY (batch_id  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_TRANSACTION_DTL}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER}
(
	country_cd    string,
	business_unit_type_cd    string,
	od_customer_type_cd    string,
	associate_id    string,
	customer_id    string,
	start_dt    date,
	end_dt    date,
	processed_dt    date
)
PARTITIONED BY (batch_id  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSIGNED_CUSTOMER}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSOCIATE}
(
	associate_id    string,
	business_channel_id    string,
	associate_first_name    string,
	associate_last_name    string,
	job_title_txt    string,
	associate_start_dt    date,
	associate_end_dt    date,
	manager_associate_id    string,
	country_cd    string,
	associate_status_ind    int,
	associate_email    string,
	associate_gender_cd    string,
	associate_cost_centre    string,
	associate_type_cd    string,
	associate_peoplesoft_id    string,
	associate_position_cd    string,
	associate_org_unit    string,
	processed_dt    date
)
PARTITIONED BY (batch_id  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_ASSOCIATE}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT}
(
	customer_id    string,
	country_cd    string,
	business_unit_type_cd    string,
	ab_comment_txt    string,
	account_established_dt    date,
	account_status_cd    string,
	account_type_cd    string,
	allow_backorder_ind    int,
	allow_split_order_ind    int,
	allow_substitute_ind    int,
	furniture_delivery_fee_ind    int,
	need_credit_approval_ind    int,
	need_return_approval_ind    int,
	od_customer_type_cd    string,
	customer_type_cd    string,
	location_id    int,
	customer_nm    string,
	set_up_channel_id    string,
	first_order_method_cd    string,
	first_order_dt    date,
	wlr_customer_ind    int,
	premier_dt    date,
	language_cd    string,
	tam_agent_id    int,
	current_credit_status_cd    string,
	spend_potential_amt    decimal(11,0),
	share_of_wallet_amt    int,
	company_size_qty    int,
	mailable_ind    int,
	avg_days_to_pay_qty    decimal(7,2),
	current_balance_amt    decimal(18,2),
	first_order_effort_id    string,
	first_order_effort_yr    int,
	first_order_premium_ind    int,
	non_rental_ind    int,
	polybag_ind    int,
	merge_id    int,
	prospect_id    int,
	credit_cd    string,
	mail_key_cd    string,
	one_person_company_ind    int,
	primary_segment_cd    string,
	mgm_ind    int,
	first_order_ship_dt    date,
	thirty_day_ind    int,
	thirty_day_amt    decimal(13,2),
	sixty_day_ind    int,
	sixty_day_amt    decimal(13,2),
	ninety_day_ind    int,
	ninety_day_amt    decimal(13,2),
	tam_target_grp_nm    string,
	tam_customer_ind    int,
	phone_ind    int,
	fax_ind    int,
	email_ind    int,
	customer_abcd_class_cd    string,
	premier_customer_cd    string,
	standard_industry_cd    string,
	branch_cd    string,
	no_of_buyers_cnt    int,
	no_of_mailable_buyers_cnt    int,
	work_life_reward_dt    date,
	customer_source_cd    string,
	source_ref_tam_agent_id    string,
	crm_id    string,
	corporate_cust_account_id    string,
	consolidated_channel_id    string,
	phone_num    string,
	postal_cd    string,
	payment_terms    string,
	loyalty_program    string,
	first_ord_value_amt    decimal(9,2),
	first_ord_cog_amt    decimal(9,2),
	first_ord_premium_costs_amt    decimal(9,2),
	internet_setup_ind    int,
	last_order_dt    date,
	last_mail_dt    date,
	last_effort_mailed    string,
	loc_sic_cd    string,
	update_dt    date,
	ika_id    string,
	duns_id    string,
	nat_id    string,
	ind_type    string,
	sales_ltv_active_month_amt    decimal(13,2),
	ltv_sales_amt    decimal(11,2),
	ltv_orders_cnt    int,
	ltv_cog_amt    decimal(11,2),
	ltv_premium_cost_amt    decimal(11,2),
	org_name2_txt    string,
	org_name3_txt    string,
	org_name4_txt    string,
	vat_number    string,
	acc_group_cd    string,
	currency_cd    string,
	delete_flag    string,
	current_record_ind    int,
	mergepurge_dt    date,
	first_invoice_dt    date,
	first_invoice_num    string,
	processed_dt    date,
	first_order_num    string,
	spend_band    int,
	ultimate_parent_id    string
)
PARTITIONED BY (batch_id  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_CUSTOMER_ACCOUNT}';



CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_GE_UNIV_SCHOBER}
(
	company_id    string,
	name_1    string,
	name_2    string,
	name_3    string,
	zipcode    string,
	town    string,
	street    string,
	po_box    string,
	zipcode_po_box    string,
	town_po_box    string,
	company_salutation    string,
	tel_area_code    string,
	telephone_no    string,
	fax_area_code    string,
	fax_no    string,
	legal_form    string,
	company_size    string,
	company_type    string,
	year_of_foundation    string,
	chamber_of_commerce_no    string,
	no_of_employees    string,
	turnover    string,
	population    string,
	capital    string,
	homepage    string,
	email    string,
	mobile_phone_no    string,
	service_phone_no    string,
	region    string,
	mailorder_activity    string,
	employee_category    string,
	activity_code    string,
	no_white_collar_workers    string,
	no_blue_collar_workers    string
)
PARTITIONED BY (batch_id  string, country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_GE_UNIV_SCHOBER}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_IR_UNIV_BILLMOSS}
(
	siteid    float,
	site_name    string,
	add1    string,
	add2    string,
	add3    string,
	add4    string,
	add5    string,
	add6    string,
	employees    float,
	sic1987_4digit    string,
	co_uid    float,
	suspend    string
)
PARTITIONED BY (batch_id  string , country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_IR_UNIV_BILLMOSS}';



CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_IT_UNIV_DBITALY}
(
	record_id    string,
	progressive_record_id    string,
	company_name    string,
	surname    string,
	name    string,
	title    string,
	address    string,
	city_town_description    string,
	province_code    string,
	postal_code    string,
	sex_code    string,
	telephone_area_code    string,
	telephone_number    string,
	movement_type    string,
	elaboration_date    string,
	activity_group_code    string,
	company_registered_status    string,
	company_typecode    string,
	yellowpages_category_code    string,
	yellowpages_description    string,
	additional_activity_descr    string,
	flag_cotel    string,
	flag_copos    string,
	flag_primary_secondary    string,
	number_of_telephone_lines    string,
	region_code_istat    string,
	province_code_istat    string,
	city_town_code_istat    string,
	street_code_seat    string,
	viking_terretorial_code    string,
	vat_number    string,
	activity_date    string,
	employee_size_code    string,
	employee_size_desc    string,
	branch    string,
	fiscal_code    string,
	scoring_light    string,
	changed_town_code    string,
	changed_town_description    string,
	changed_zip_code    string,
	changed_street_code    string,
	changed_address    string,
	changed_comp_reg    string,
	changed_phone_number    string,
	changed_phone_lines    string,
	changed_yellowpages_code    string,
	changed_add_activity_descr    string,
	changed_employee_size    string,
	changed_scoring_light    string,
	changed_sede_principal    string,
	changed_vat_number    string,
	changed_fiscal_code    string,
	telephone_id    string,
	registration_date    date
)
PARTITIONED BY (batch_id  string , country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_IT_UNIV_DBITALY}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_AT_UNIV_KSV}
(
	herold_id    string,
	ksv_id    string,
	name    string,
	street    string,
	zipcode    string,
	city    string,
	no_of_employees    string,
	no_white_collar_workers    string,
	no_blue_collar_workers    string,
	company_name    string,
	company_name_ksv    string,
	uid_no    string,
	telephone_1    string,
	telephone_2    string,
	telephone_3    string,
	telephone_4    string,
	telephone_5    string,
	e_mail_1    string,
	e_mail_2    string,
	e_mail_3    string,
	e_mail_4    string,
	e_mail_5    string,
	cat_branch_group_1    string,
	cat_branch_group_2    string,
	cat_branch_group_3    string,
	cat_branch_group_4    string,
	cat_branch_group_5    string,
	at_nace_branch_no_1    string,
	at_nace_branch_no_2    string,
	at_nace_branch_no_3    string,
	at_nace_branch_no_4    string,
	at_nace_branch_no_5    string,
	at_nace_branch_1    string,
	at_nace_branch_2    string,
	at_nace_branch_3    string,
	at_nace_branch_4    string,
	at_nace_branch_5    string
)
PARTITIONED BY (batch_id  string , country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_AT_UNIV_KSV}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_AT_UNIV_SCHOBER}
(
	company_id    string,
	salutation    string,
	title    string,
	first_name    string,
	last_name    string,
	company_name_1    string,
	company_name_2    string,
	company_name_3    string,
	street    string,
	street_postalcode    string,
	town_po_box    string,
	po_box    string,
	po_box_postalcode    string,
	po_box_city    string,
	phone_area_code    string,
	phone_number    string,
	fax_area_code    string,
	fax_number    string,
	type_of_company    string,
	legal_form    string,
	no_of_employees    string,
	company_size    string,
	no_of_beds    string,
	no_chamber_of_commerce    string,
	year_foundation    string,
	business_capital    string,
	turnover    string,
	size_of_city    string,
	ebc_code    string,
	email    string,
	homepage    string,
	shared_practice    string,
	mobilphone    string,
	typ_wgs84    string,
	x_geo_wgs84    string,
	y_geo_wgs84    string,
	typ_lambert    string,
	x_geo_lambert_konf    string,
	y_geo_lambert_konf    string,
	nace_2008    string,
	nace_2008_desc    string,
	blue_collar_worker    string,
	white_collar_worker    string,
	growth_indicator    string,
	flag_score    string
)
PARTITIONED BY (batch_id  string,country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_AT_UNIV_SCHOBER}';



CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_BE_UNIV_SCHOBER}
(
	be_id    string,
	be_code    string,
	btw_number    string,
	company_name    string,
	legal_form    string,
	street    string,
	house_number    string,
	house_number_add    string,
	postcode    string,
	city    string,
	legal_district    string,
	province    string,
	language    string,
	nace_bel    string,
	main_activity_code    string,
	activity_code2    string,
	activity_code3    string,
	activity_code4    string,
	activity_code5    string,
	phone_number    string,
	fax_number    string,
	employee_size    string,
	date_of_foundation    date,
	ceo_name    string,
	ceo_first_name    string,
	ceo_sex    string,
	ceo_language    string,
	mng_dir_name    string,
	mng_dir_first_name    string,
	mng_dir_sex    string,
	mng_dir_language    string,
	internal_schober    string,
	mng_pur    string,
	mng_pur_first_name    string,
	mng_pur_sex    string,
	mng_pur_language    string,
	mng_hr    string,
	mng_hr_first_name    string,
	mng_hr_sex    string,
	mng_hr_language    string,
	mng_it    string,
	mng_it_first_name    string,
	mng_it_sex    string,
	mng_it_language    string,
	dir_sales    string,
	dir_sales_first_name    string,
	dir_sales_sex    string,
	dir_sales_language    string,
	mng_mkt    string,
	mng_mkt_first_name    string,
	mng_mkt_sex    string,
	mng_mkt_language    string,
	dir_financ    string,
	dir_financ_first_name    string,
	dir_financ_sex    string,
	dir_financ_language    string,
	email    string,
	website    string,
	turnover    string,
	import1    string,
	export    string,
	company_type    string,
	employees    string,
	white_collar_class    string,
	ebi_number    string,
	filler    string
)
PARTITIONED BY (batch_id  string, country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_BE_UNIV_SCHOBER}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_FR_MDUGAST_N80FIN}
(
	siren    string,
	nic    string,
	l1_nomen    string,
	l2_comp    string,
	l3_cadr    string,
	l4_voie    string,
	l5_disp    string,
	l6_post    string,
	l7_etrg    string,
	zr1    string,
	rpet    string,
	depet    string,
	arronet    string,
	ctonet    string,
	comet    string,
	libcom    string,
	du    string,
	tu    string,
	uu    string,
	codpos    string,
	zr2    string,
	tcd    string,
	zemet    string,
	codevoie    string,
	numvoie    string,
	indrep    string,
	typevoie    string,
	libvoie    string,
	enseigne    string,
	apet700    string,
	apet31    string,
	siege    string,
	tefet    string,
	efetcent    string,
	origine    string,
	dcret    string,
	mmintret    string,
	activnat    string,
	lieuact    string,
	actisurf    string,
	saisonat    string,
	modet    string,
	dapet    string,
	defet    string,
	explet    string,
	prodpart    string,
	auxilt    string,
	eaeant    string,
	eaeapet    string,
	eaesec1t    string,
	eaesec2t    string,
	nomen    string,
	sigle    string,
	civilite    string,
	cj    string,
	tefen    string,
	efencent    string,
	apen700    string,
	apen31    string,
	aprm    string,
	tca    string,
	recme    string,
	dapen    string,
	defen    string,
	dcren    string,
	mmintren    string,
	monoact    string,
	moden    string,
	explen    string,
	eaeann    string,
	eaeapen    string,
	eaesec1n    string,
	eaesec2n    string,
	eaesec3n    string,
	eaesec4n    string,
	nbetexpl    string,
	tcaexp    string,
	regimp    string,
	monoreg    string,
	rpen    string,
	depcomen    string,
	vmaj    string,
	vmaj1    string,
	vmaj2    string,
	vmaj3    string,
	siret    string,
	maj    string,
	dmaj    string,
	eve    string,
	typetab    string,
	tel    string,
	dateve    string,
	exp_is_actif    string
)
PARTITIONED BY (batch_id  string , country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_FR_MDUGAST_N80FIN}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_NL_UNIV_VIKBASE}
(
	code    string,
	vnum	string,
	rnum	string,
	cham	string,
	naam	string,
	nama	string,
	sstr	string,
	huis	string,
	hstv	string,
	post	string,
	plac	string,
	pstr	string,
	pbus	string,
	posp	string,
	plap	string,
	prov	string,
	hbrn	string,
	tbrn	string,
	beco	string,
	bgrt	string,
	revo	string,
	hffi	string,
	teln	string,
	tela	string,
	faxn	string,
	faxa	string,
	grow	string,
	soho	string,
	recl	string,
	rere	string,
	ecac	string,
	novi	string,
	fdat	string,
	inar	string,
	gemc	string,
	eggc	string,
	coro	string,
	cebu	string,
	niel	string,
	inwo	string,
	impc	string,
	expc	string,
	part	string,
	chab	string,
	updated    string,
	filler    string,
	email    string,
	wcclass    string,
	bvg    string,
	nomail    string
)
PARTITIONED BY (batch_id  string,country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_NL_UNIV_VIKBASE}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_UK_UNIV_LBMDATA}
(
	lbm_diallerid    string,
	marketdata_urn    string,
	company    string,
	add1    string,
	add2    string,
	add3    string,
	add4    string,
	town    string,
	county    string,
	postcode    string,
	pcarea    string,
	pcdist    string,
	postcoderegion    string,
	telephone    string,
	fax    string,
	emps    string,
	emps_band    string,
	emps_band_large    string,
	total_emps    string,
	total_emps_band_large    string,
	sic5    string,
	sic5_description    string,
	sic4    string,
	sic4_description    string,
	sic2    string,
	sic2_description    string,
	sicsector    string,
	sicsector_description    string,
	thomson_code    string,
	thomson_code_description    string,
	lbm5    string,
	lbm5_description    string,
	sitetype    string,
	no_sites    string,
	tps    string,
	tpsc    string,
	fps    string,
	mps    string,
	estdate    string,
	lbm_businesstype    string,
	lbm_businesstype_description    string,
	company_verifieddate    string,
	companylevel_turnoverband    string,
	comp_turnoverband_descr    string,
	recency_18mths    string,
	recency_12mths    string,
	months_recency    string,
	siteparentid    string,
	lbm_siteparentdiallerid    string,
	update_type    string,
	last_update_lbm    date,
	edw_load_date    date,
	edw_amend_date    date,
	sic2_2007    string,
	sic2_2007_desc    string,
	sic3_2007    string,
	sic3_2007_desc    string,
	sic4_2007    string,
	sic4_2007_desc    string,
	sic5_2007    string,
	sic5_2007_desc    string
)
PARTITIONED BY (batch_id  string , country_code  string)
Row format delimited fields terminated by '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_UK_UNIV_LBMDATA}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_FR}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_FR}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_NL}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_NL}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_GE}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_GE}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_BE}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_BE}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_IT}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_IT}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_AT}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_AT}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_UK}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_UK}';

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_WORK}.${hiveconf:TBL_WORK_CDMPRDDTA_DM_IR}
(
	country_cd string,
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	trx_date string,
	associate_full_name string,
	transaction_id string,
	net_sales float,
	big_deal float,
	cogs float
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '${hiveconf:WORK_HDFS}/${hiveconf:TBL_WORK_CDMPRDDTA_DM_IR}';

--############################  GOLD Layer #####################################

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_CDMPRDDTA_DM}
(
	fiscal_month string,
	fiscal_year string,
	customer_id string,
	customer_nm string,
	associate_id string,
	associate_full_name string,
	tot_revenue double,
	big_deal double,
	cogs double
)
PARTITIONED BY (batch_id string,country_cd string)
STORED AS PARQUET
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_CDMPRDDTA_DM}';


CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_GOLD}.${hiveconf:TBL_GOLD_UNIV}
(
	id string,
	company string,
	postcode string,
	region string,
	emps_band string,
	total_no_of_employees string,
	sic_nace_description string,
	sic_nace_description_2 string,	
	sic_nace_description_4 string,
	sic_nace_description_5 string,
	turnover string,
	spend_potential string
)
PARTITIONED BY (batch_id string, country_cd string)
STORED AS PARQUET
LOCATION '${hiveconf:GOLD_HDFS}/${hiveconf:TBL_GOLD_UNIV}';


------############## AUDIT LAYER #############-------------------

--# job_statistics #

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_AUDIT}.${hiveconf:TBL_JOB_STATISTIC}
(
  batchId                                        string,
  jobname                                        string,
  tableName                                      string,
  user                                           string,
  startTime                                      string,
  endTime                                        string,
  totalTime                                      string,
  noOfRecords                                    string,
  status                                         string,
  recordType                                     string,
  logPath                                        string
)
PARTITIONED BY (current_date  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '${hiveconf:AUDITLOG_HDFS}/${hiveconf:TBL_JOB_STATISTIC}';

--##############################################################################
--#                                    End                                     #
--##############################################################################

