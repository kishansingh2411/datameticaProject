--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : outgoing_clickstream_view_daily           #
--# File                                                                       #
--#     : rollups.pig.                                                   #
--# Description                                                                #
--#     : Rollup script to store data from outgoing to hbase tables:           #
--#  weekly/monthly/quarterly/yearly respectively as per request          #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Shweta karwa / Sarfarazkhan                               #
--#                                                                            #
--##############################################################################
--#                                   Declare                                  #
--##############################################################################

SET record_type.name '$ROLLUP_SSH_ACTION';
  %declare CURR_DATE `date +"%Y%m%d"`;

--##############################################################################
--#                                   Load                                     #
--##############################################################################

-- Loading data from outgoing_clickstream_view_daily.

rollup_load_data = 
   LOAD '$OUTPUT_DIRECTORY/daily/'
   USING PigStorage(',') AS
   (day:int,
   country:chararray,
   visitor_segment:chararray,
   total_orders:double,
   total_visits:int,
   total_page_views:int,
   total_rev_frm_ncust:double,
   total_rev_frm_ecust:double,
   total_cart_started:int,
   total_cart_aband:int,
   total_view_products:int,
   total_rev_frm_anymcust:double);
   
--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- substituting alias for all required dates
rollup_generating_dates = 
   FOREACH rollup_load_data GENERATE *,
   (chararray)GetWeek(ToDate((chararray)'$CURR_DATE','yyyyMMdd'))  as day_week,
   (chararray)GetYear(ToDate((chararray)'$CURR_DATE','yyyyMMdd')) as day_year,
   (chararray)GetWeek(ToDate((chararray)day,'yyyyMMdd'))   as input_day_week,
   (chararray)GetYear(ToDate((chararray)day,'yyyyMMdd')) as input_day_year;
   

rollup_generating_week = FOREACH rollup_generating_dates GENERATE *, ((int) input_day_week <10 ? CONCAT('0',input_day_week) : input_day_week ) as new_input_day_week;
					
rollup_new_date_variables = 
    FOREACH rollup_generating_week GENERATE *,
    CONCAT(input_day_year,new_input_day_week) as new_date_week;

rollup_display_data = 
   FOREACH rollup_new_date_variables GENERATE
   day,
   country,
   visitor_segment,
   (double)total_orders as total_orders,
   total_visits,
   total_page_views,
   (double)total_rev_frm_ncust as total_rev_frm_ncust,
   (double)total_rev_frm_ecust as total_rev_frm_ecust,
   total_cart_started,
   total_cart_aband,
   total_view_products,
   (double)total_rev_frm_anymcust as total_rev_frm_anymcust,
   (chararray)day_week,
   (chararray)day_year,
   (chararray)input_day_week,
   (chararray)input_day_year,
   new_date_week;
   
rollup_display_data_filter= FILTER rollup_display_data BY ('$record_type' == 'weekly' AND input_day_week == day_week AND input_day_year == day_year);

-- generating only required fields according to the request with first column as yyyyweeknumber/yyyymonthnumber/yyyyquarternumber/yyyy

rollup_typecasted_outgoing_data =
   FOREACH rollup_display_data_filter GENERATE
   new_date_week,
   country,
   visitor_segment,
   total_orders,
   total_visits,
   total_page_views,
   total_rev_frm_ncust,
   total_rev_frm_ecust,
   total_cart_started,
   total_cart_aband,
   total_view_products,
   total_rev_frm_anymcust;
   
-- grouping the filtered data

rollup_grouping_final_data = 
   GROUP rollup_typecasted_outgoing_data BY (new_date_week,country,visitor_segment);


-- Flattening the bag to tuples

rollup_flatenned_data =
   FOREACH rollup_grouping_final_data GENERATE
   FLATTEN(rollup_typecasted_outgoing_data),
   SUM(rollup_typecasted_outgoing_data.total_orders) AS SUM_total_orders,
   SUM(rollup_typecasted_outgoing_data.total_visits) AS SUM_total_visits,
   SUM(rollup_typecasted_outgoing_data.total_page_views) AS SUM_total_page_views,
   SUM(rollup_typecasted_outgoing_data.total_rev_frm_ncust) AS SUM_total_rev_frm_ncust,
   SUM(rollup_typecasted_outgoing_data.total_rev_frm_ecust) AS SUM_total_rev_frm_ecust,
   SUM(rollup_typecasted_outgoing_data.total_cart_started) AS SUM_total_cart_started,
   SUM(rollup_typecasted_outgoing_data.total_cart_aband) AS SUM_total_cart_aband,
   SUM(rollup_typecasted_outgoing_data.total_view_products) AS SUM_total_view_products,
   SUM(rollup_typecasted_outgoing_data.total_rev_frm_anymcust) AS SUM_total_rev_frm_anymcust,
   COUNT(rollup_typecasted_outgoing_data) as count_s; 

-- Generating only required fields

rollup_required_fields = 
   FOREACH rollup_flatenned_data generate
   new_date_week,
   country,
   visitor_segment,
   SUM_total_orders,
   SUM_total_visits,
   SUM_total_page_views,
   SUM_total_rev_frm_ncust,
   SUM_total_rev_frm_ecust,
   SUM_total_cart_started,
   SUM_total_cart_aband,
   SUM_total_view_products,
   SUM_total_rev_frm_anymcust;
  

--   fetching unique records

rollup_final_data = 
   DISTINCT rollup_required_fields;


--##############################################################################
--#                                   Store                                    #
--##############################################################################

    STORE rollup_final_data
    INTO '$OUTPUT_DIRECTORY/$record_type/$batch_id/' 
    USING PigStorage(',');
--##############################################################################
--#                                    End                                     #
--##############################################################################
  