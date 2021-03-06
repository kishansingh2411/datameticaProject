--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_rsdvr_preferences_delete_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 05/05/2016
--#   Log File    : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/05/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(  
	SCHEDULE_ID                     BIGINT   COMMENT '',
	ASSET_ID                 BIGINT   COMMENT '',
	HOME_ID               VARCHAR(255)   COMMENT '',
	RECORDING_ID           BIGINT          COMMENT ''  ,
	CREATE_TIME                 TIMESTAMP      COMMENT '',
	OP_TYPE                     VARCHAR(1)     COMMENT '',
	DTM_CREATED                 TIMESTAMP      COMMENT '',
	RSDVR_RECORDINGS_DELTA_SEQ BIGINT         COMMENT ''
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################