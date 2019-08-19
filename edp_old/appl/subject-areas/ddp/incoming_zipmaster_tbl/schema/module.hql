--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_zipmaster_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2015
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
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name} 
 ( 
	CITY                 VARCHAR(18)               COMMENT 'CITY NAME TIED TO THIS ZIP',
	CORP                 INT                       COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP.',
	CYCLE                VARCHAR(1)                COMMENT 'CUSTOMER BILLING CYCLE ASSIGNED TO ZIP CODE RECORD.VALID CYCLES FOR CORP ARE FOUND IN TABLE CYCLETABLE',
	LOAD_DATE            TIMESTAMP                 COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE',
	MACHINE_ID           VARCHAR(5)                COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.',
	OP_TYPE              VARCHAR(1)                COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIPBES THE ORIGINAL OPERATION THE',
	STATE                VARCHAR(2)                COMMENT 'TWO CHARACTER STATE ABBREVIATION',
	ZIPBEG               VARCHAR(6)                COMMENT 'ZIP CODE BEGINNING RANGE',
	ZIPEND               VARCHAR(6)                COMMENT 'ENDING RANGE OF THIS GROUP OF ZIP CODES',
	ZIPMASTER_SEQ        BIGINT                       COMMENT ''
)
PARTITIONED BY (LOAD_YEAR STRING, 
                LOAD_MONTH STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################