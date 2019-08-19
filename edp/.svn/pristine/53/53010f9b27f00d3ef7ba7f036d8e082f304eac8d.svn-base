--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_custrates_tbl)
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
	CORP                 INT              COMMENT 'Five digit system number assigned to each corp.',
	`PARTITION`          VARCHAR(2)       COMMENT 'Data hashing value for database management - equals the last 2 digits of the house #',
	HOUSE                VARCHAR(6)       COMMENT 'House number, Account Number, Alpha/Numeric, Refer to Apendix_doc #2 ',
	CUST                 VARCHAR(2)       COMMENT 'Customer number. Part of the Customer account number.  Refer to Appendix_doc #2.',
	SERV                 VARCHAR(2)       COMMENT 'Rate code, RATE Table',
	RTCNT                INT              COMMENT 'Rate code count',
	PRICELEVEL           INT              COMMENT '',
    MACHINE_ID           VARCHAR(5)       COMMENT 'The id of the tandem that the original record was extracted from.',
	OP_TYPE              VARCHAR(1)       COMMENT 'Golden Gate Assigned operation type that descripbes the original operation the',
	LOAD_DATE            TIMESTAMP        COMMENT 'Oracle assigned date of when the record was loaded into the Oracle Database',
	CUSTRATES_SEQ        BIGINT           COMMENT '',
	GGS_TRANS_TIMESTAMP  VARCHAR(26)      COMMENT 'Golden Gate assigned date for which the transaction occurred on the Tandem'
)
PARTITIONED BY (LOAD_YEAR STRING,
                LOAD_MONTH STRING,
                LOAD_DAY STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################