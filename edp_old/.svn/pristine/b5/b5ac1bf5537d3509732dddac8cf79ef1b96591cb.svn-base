--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Incoming table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/channel_tuning/
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE 
   TABLE ${hivevar:target_hive_database}.${hivevar:target_table}
   SELECT
	 day_tag ,
	 period_id ,
	 period_date ,
	 year ,
	 quarter_nbr ,
	 month_nbr ,
	 day_of_the_year ,
	 day_of_month ,
	 quarter_nbr_overall ,
	 month_nbr_overall ,
	 week_nbr_overall ,
	 day_nbr_overall ,
	 week_nbr_in_year ,
	 day_of_week ,
	 day_of_week_abbr ,
	 fiscal_year ,
	 fiscal_quarter ,
	 fiscal_month_nbr ,
	 fiscal_month_nbr_overall ,
	 month ,
	 month_abbr ,
	 fiscal_qtr_overall_tag ,
	 quarter_overall_tag ,
	 month_overall_tag,
	 last_day_in_month_ind ,
	 last_day_of_quarter_ind ,
	 us_holiday_ind ,
	 canadian_holiday_ind ,
	 prev_month_start_period_id ,
	 prev_mtd_period_id ,
	 prev_qtr_start_period_id ,
	 prev_qtd_period_id ,
	 prev_year_start_period_id ,
	 prev_ytd_period_id ,
	 prev_month_period_id ,
	 prev_quarter_period_id ,
	 prev_year_period_id ,
	 month_tag ,
	 dtm_created ,
	 dtm_last_updated ,
	 week_nbr_overall_mon_to_sun ,
	 week_nbr_overall_tue_to_mon ,
	 week_nbr_overall_wed_to_tue ,
	 week_nbr_overall_thu_to_wed ,
	 week_nbr_overall_fri_to_thu ,
	 week_nbr_overall_sat_to_fri ,
	 broadcast_year ,
	 broadcast_month_nbr_of_year ,
	 broadcast_week_nbr_of_year ,
	 broadcast_day_nbr_of_year ,
	 broadcast_day_nbr_of_week ,
	 broadcast_day_nbr_of_month ,
	 broadcast_week_desc ,
	 calendar_week_desc ,
	 broadcast_month_desc ,
	 tgt_dtm_created ,
	 tgt_dtm_last_updated ,
	 broadcast_week_id ,
	 broadcast_month_id ,
	 calendar_week_id ,
	 calendar_month_id ,
	 day_of_week_id
FROM 
	 ${hivevar:src_hive_database}.${hivevar:src_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################