--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(work_rsdvr_requests_tbl)
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
(	ID_SCHEDULE               BIGINT,
	ID_HOME                   VARCHAR(32),
	SERIALNUM                 VARCHAR(32),
	MACADDRESS                VARCHAR(32),
	CALLSIGN                  VARCHAR(12),
	STARTDATE                 TIMESTAMP,
	STARTTIME                 VARCHAR(8),
	DURATION                  INT,
	REC_TYPE                  INT,
	SAVE_ALL                  INT,
	SAVE_LATEST               INT,
	CREATE_DATE               TIMESTAMP,
	MODIFY_TIME               TIMESTAMP,
	ENDTIME_ADJUSTMENT        INT,
	SAVE_DAYS				  DECIMAL(5, 0),
	STATUS					  BIGINT,
	SHORT_TITLE				  VARCHAR(128),
	DS_STARTTIME			  TIMESTAMP,
	LAST_UPDATED			  TIMESTAMP,
	LOADTIME                  TIMESTAMP
    )
STORED AS TEXTFILE    
LOCATION '${hivevar:location}'
;
--##############################################################################
--#                                    End                                     #
--##############################################################################