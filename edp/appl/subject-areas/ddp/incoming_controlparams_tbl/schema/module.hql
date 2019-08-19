--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_controlparams_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 05/02/2016
--#   Log File    : .../log/DDP_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/DDP_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/02/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name} 
( 
	ACCTCORP           INT                COMMENT 'Home account corp number, All 5 digits of the corp number',
	CORP               INT                COMMENT 'Four Digit system number assigned to each corp. The corp # is unique only within one Tandem system.',
	DNAME              VARCHAR(22)        COMMENT 'Business name of system',
	MACHINE_ID         VARCHAR(5)         COMMENT 'The id of the tandem that the original record was extracted from.',
	OP_TYPE            VARCHAR(1)         COMMENT 'Golden Gate Assigned operation type that descripbes the original operation the ',
	LOAD_DATE          TIMESTAMP          COMMENT 'Oracle assigned date of when the record was loaded into the Oracle Database',
	CONTROLPARAMS_SEQ  BIGINT             COMMENT ' '
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################