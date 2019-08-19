--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_t_f_household_device_tbl)
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
    HOUSEHOLD_DEVICE_ID	         int             COMMENT '',
	STB_FILE_CONTROL_ID	         int             COMMENT '',
	IP_ADDRESS	                 VARCHAR(32)     COMMENT '',
	SUBNET_IP_ADDRESS	         VARCHAR(19)     COMMENT '',
	NODE_IDENTIFICATION	         int             COMMENT '',
	ZIP_CODE	                 VARCHAR(10)     COMMENT '', 
	MISC_OTHER_ATTRIBUTION	     VARCHAR(2000)   COMMENT '',
	LOAD_DATE	                 timestamp       COMMENT '',
	DTM_CREATED	                 timestamp       COMMENT '',
	SEQ	                         int             COMMENT '',
	CSEQ	                     int             COMMENT '',
	TGT_DTM_CREATED	             timestamp       COMMENT '',
	TGT_DTM_LAST_UPDATED         timestamp       COMMENT ''
)
PARTITIONED BY (LOAD_MONTH STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################