--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_t_cvb_chdv_a_hh_series_summary_hst_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/30/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(         
    HOUSEHOLD_ID             int           COMMENT '',
    PLAYBACK_VIEWING_AGGR_ID int           COMMENT '',
	CUMULATIVE_PLAYBACK_DAYS int           COMMENT '',
	TUNING_TYPE              VARCHAR(30)   COMMENT '',
	TUNING_SOURCE            VARCHAR(30)   COMMENT '',
	CHANNEL_NAME_ID          int           COMMENT '',
	TMS_PROGRAM_ID           int           COMMENT '',
	SAMPLE_ID                int           COMMENT '',
	TITLE                    VARCHAR(400)  COMMENT '',
	YEAR_WEEK                int           COMMENT '',
	VIEWING_DRTN             int           COMMENT '',
	VIEWING_CNT              int           COMMENT '',
	DEVICE_COUNT             int           COMMENT '',
	DTM_CREATED              timestamp     COMMENT '',
	DTM_LAST_UPDATED         timestamp     COMMENT '',
	DTM_MODIFIED             timestamp     COMMENT '',
	SOURCED_FROM_SYSTEM      VARCHAR(30)   COMMENT '',
	LAST_UPDATED_BY_SYSTEM   VARCHAR(400)  COMMENT ''
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '~' 
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################