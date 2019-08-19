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
	 tms_program_id ,
	 tms_database_key ,
	 program_classification ,
	 title ,
	 reduced_title_1 ,
	 reduced_title_2 ,
	 reduced_title_3 ,
	 reduced_title_4 ,
	 alt_title ,
	 reduced_descr_1 ,
	 reduced_descr_2 ,
	 reduced_descr_3 ,
	 adult_situation ,
	 graphic_language ,
	 nudity ,
	 violence ,
	 sexual_content ,
	 consentual_behavior ,
	 descr_1 ,
	 year ,
	 mpaa_rating ,
	 star_rating ,
	 run_time ,
	 color_code ,
	 program_language ,
	 country_of_origin ,
	 made_for_tv ,
	 source_type ,
	 show_type ,
	 holiday ,
	 syndicated_episode_nbr ,
	 alt_syndicated_episode_nbr ,
	 episode_title ,
	 user_data_1 ,
	 user_data_2 ,
	 descr_2 ,
	 reduced_descr ,
	 org_studio ,
	 game_date ,
	 game_time ,
	 game_time_zone ,
	 org_air_date ,
	 user_data_3 ,
	 dtm_created ,
	 dtm_last_updated ,
	 sourced_from_system ,
	 last_updated_by_system ,
	 tgt_dtm_created ,
	 tgt_dtm_last_updated ,
	 rootid ,
	 versionid ,
	 connectorid ,
	 seasonid ,
	 seriesid ,
	 ei_season ,
	 ei_number ,
	 ei_episodes
FROM 
	 ${hivevar:src_hive_database}.${hivevar:src_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################