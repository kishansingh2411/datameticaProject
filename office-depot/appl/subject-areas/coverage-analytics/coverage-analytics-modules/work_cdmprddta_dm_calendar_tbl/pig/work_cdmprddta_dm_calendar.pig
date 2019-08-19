--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cdmprddta_dm_calendar                                      	   #
--# File                                                                       #
--#     : work_cdmprddta_dm_calendar.pig                                       #
--# Description                                                                #
--#     : To load data into outgoing layer                                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Sonali Rawool                                                        #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################
Register $OPENCSV_JAR;

SET job.name '$WORK_CDMPRDDTA_DM_CALENDAR_SSH_ACTION';

work_cdmprddta_dm_calender = 
	LOAD '$DB_INCOMING.$INCOMING_CDMPRDDTA_DM_CALENDAR'
    USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_cdmprddta_dm_calender_generate = 
	FOREACH work_cdmprddta_dm_calender GENERATE
    RTRIM(calendar_dt),
	RTRIM(fiscal_week),
	RTRIM(fiscal_month),
	RTRIM(fiscal_quarter),
	RTRIM(fiscal_half),
	RTRIM(fiscal_year),
	RTRIM(fiscal_period),
	RTRIM(day),
	RTRIM(day_number),
	RTRIM(day_of_year),
	RTRIM(month_end),
	RTRIM(fiscal_month_desc),
	RTRIM(season),
	RTRIM(previous_day),
	RTRIM(date_last_week),
	RTRIM(date_last_month),
	RTRIM(date_last_year),
	RTRIM(date_end_of_previous_period),
	RTRIM(date_same_weekday_last_year),
	RTRIM(last_week_end_date),
	RTRIM(same_period_last_year),
	RTRIM(day_of_fiscal_month),
	RTRIM(processed_dt),
	RTRIM(calendar_month),
	RTRIM(calendar_year);
              
--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_cdmprddta_dm_calender_generate 
	INTO '$WORK_HDFS/$WORK_CDMPRDDTA_DM_CALENDAR/batch_id=$batch_id/'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################