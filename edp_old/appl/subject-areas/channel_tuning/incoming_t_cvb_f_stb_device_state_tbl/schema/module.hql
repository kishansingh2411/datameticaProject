--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_t_cvb_stb_device_state_tbl)
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
    HOUSEHOLD_DEVICE_ID	       int             COMMENT '',
	STB_DEVICE_STATE_ID	       int             COMMENT '',
	FROM_DAY_TAG	           VARCHAR(8)      COMMENT '',
	FROM_DATE	               date            COMMENT '',
	FROM_DAY_TIME	           VARCHAR(8)      COMMENT '',
	DURATION_IN_SECONDS_AMT	   bigint          COMMENT '',
	INTERVAL_CNT	           int             COMMENT '',
	INTERVAL_IN_SECONDS_AMT	   int             COMMENT '',
	STB_FILE_CONTROL_ID	       int             COMMENT '',
	DTM_FROM	               timestamp       COMMENT '',
	DTM_TO	                   timestamp       COMMENT '',
	LOAD_DATE	               timestamp       COMMENT '',
	DTM_CREATED	               timestamp       COMMENT '',
	STB_COLLECTION_TASK_ID	   int             COMMENT '',
	A_STB_DEVICE_STATE_ID	   bigint          COMMENT '',
	CSEQ	                   int             COMMENT '',
	SEQ	                       int             COMMENT '',
	TGT_DTM_CREATED	           timestamp       COMMENT '', 
	TGT_DTM_LAST_UPDATED       timestamp       COMMENT ''
)
PARTITIONED BY(LOAD_MONTH STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################