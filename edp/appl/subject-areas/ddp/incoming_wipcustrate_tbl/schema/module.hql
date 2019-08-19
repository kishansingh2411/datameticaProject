--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_wipcustrate_tbl)
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
	CORP                       INT                COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP.',
	`PARTITION`                VARCHAR(2)         COMMENT 'DATA HASHING VALUE FOR DATABASE MANAGEMENT - EQUALS THE LAST 2 DIGITS OF THE HOUSE #',
	HOUSE                      VARCHAR(6)         COMMENT 'HOUSE NUMBER, ACCOUNT NUMBER, ALPHA/NUMERIC, REFER TO APENDIX_DOC #2',
	CUST                       VARCHAR(2)         COMMENT 'CUSTOMER NUMBER. PART OF THE CUSTOMER ACCOUNT NUMBER.  REFER TO APPENDIX_DOC #2.',
	WPCNT                      VARCHAR(1)         COMMENT 'IS A COUNTER TO LINK WIP RECORDS.A WPCNT OF IS 5IS  WOULD LINK ALL RECORDS WITH WPCNT 5 TO ONE TRANSACTN, THE NEXT TRANSACTION WOULD HAVE WPCNT IS 6IS  AND SO FORTH. LINKS THE HOUSE, WIP, WIP COMMENT',
	RATESIGN                   VARCHAR(1)         COMMENT 'SIGN FOR RATE CODE,ADDING OR SUBTRACTING, +,-',
	RATECD                     VARCHAR(2)         COMMENT 'RATE CODE ASSOCIATED WITH THIS CHARGE LEVEL, RATE TABLE',
	SERCNT                     INT                COMMENT 'NUMBER OF TIMES TO CHARGE THIS RATE CODE',
	SERTY                      VARCHAR(1)         COMMENT 'TYPE OF RATE CODE, S=SERVICE,R=RENT,I=INSTAL L,D=DEPOSIT',
	SAMT                       DECIMAL(6,2)       COMMENT 'AMOUNT TO CHARGE FOR THIS RATE CODE',
	MACHINE_ID                 VARCHAR(5)         COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.',
	OP_TYPE                    VARCHAR(1)         COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIPBES THE ORIGINAL OPERATION THE',
	LOAD_DATE                  TIMESTAMP          COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE',
	WIPCUSTRATE_SEQ            BIGINT             COMMENT '',
	GGS_TRANS_TIMESTAMP        VARCHAR(26)        COMMENT 'GOLDEN GATE ASSIGNED DATE FOR WHICH THE TRANSACTION OCCURRED ON THE TANDEM',
    PRICELEVEL                 INT                COMMENT ''	
)
PARTITIONED BY (LOAD_YEAR  STRING, 
                LOAD_MONTH STRING, 
                LOAD_DAY   STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
; 

--##############################################################################
--#                                    End                                     #
--##############################################################################