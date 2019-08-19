--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_wipoutlet_tbl)
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
	ADDLEQ              VARCHAR(8)                COMMENT 'ANY ADDITIONAL EQUIPMENT INFORMATION PERTAINING TOTHBOX, SUCH AS REMOTE CONTROL NUMBER   ',
	BOXLOC              VARCHAR(1)                COMMENT 'OUTLET LOCATION CODE, CODE36 TABLE 120      ',
	BXRET               VARCHAR(1)                COMMENT 'BOX RETURN REASON CODE, CODE36 TABLE 128    ',
	CHNGPRG       		VARCHAR(1)                COMMENT 'PROGRAM THAT MADE THE CHANGE 1=WIP, 2=T/C, 3=SRO, *=OPT. 13, +=BOX MAINT   ',
	CORP          		INT                       COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP.',
	CUST          		VARCHAR(2)                COMMENT 'CUSTOMER NUMBER. PART OF THE CUSTOMER ACCOUNT NUMBER.  REFER TO APPENDIX_DOC #2.    ',
	EMGFLG        		VARCHAR(1)                COMMENT 'EMERGENCY AUTHORIZATION FLAG ON THOUTLET     ',
	FIXED01       		VARCHAR(2)                COMMENT 'USER DEFINED CODES IDENTIFYING FIX CODE95 TABLE 204,205    ',
	FIXED02       		VARCHAR(2)                COMMENT 'USER DEFINED CODES IDENTIFYING FIX CODE95 TABLE 204,205    ',
	FIXED03       		VARCHAR(2)                COMMENT 'USER DEFINED CODES IDENTIFYING FIX CODE95 TABLE 204,205    ',
	FIXED04       		VARCHAR(2)                COMMENT 'USER DEFINED CODES IDENTIFYING FIX CODE95 TABLE 204,205    ',
	FIXED05        		VARCHAR(2)                COMMENT 'USER DEFINED CODES IDENTIFYING FIX CODE95 TABLE 204,205    ',
	FIXED06        		VARCHAR(2)                COMMENT 'USER DEFINED CODES IDENTIFYING FIX CODE95 TABLE 204,205    ',
	GGS_TRANS_TIMESTAMP VARCHAR(26)               COMMENT 'GOLDEN GATE ASSIGNED DATE FOR WHICH THE TRANSACTION OCCURRED ON THE TANDEM      ',
	HOUSE        		VARCHAR(6)                COMMENT 'HOUSE NUMBER, ACCOUNT NUMBER, ALPHA/NUMERIC, REFER TO APENDIX_DOC #2    ',
	LOAD_DATE        	TIMESTAMP                 COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE     ',
	MACHINE_ID        	VARCHAR(5)                COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.       ',
	NEWFIXED01        	VARCHAR(3)                COMMENT '',
	NEWFIXED02        	VARCHAR(3)                COMMENT '',
	NEWFIXED03        	VARCHAR(3)                COMMENT '',
	NEWFIXED04        	VARCHAR(3)                COMMENT '',
	NEWFIXED05        	VARCHAR(3)                COMMENT '',
	NEWFIXED06        	VARCHAR(3)                COMMENT '',
	NEWTASK01        	VARCHAR(3)                COMMENT '',
	NEWTASK02        	VARCHAR(3)                COMMENT '',
	NEWTASK03        	VARCHAR(3)                COMMENT '',
	NEWTASK04        	VARCHAR(3)                COMMENT '',
	NEWTASK05        	VARCHAR(3)                COMMENT '',
	NEWTASK06        	VARCHAR(3)                COMMENT '',
	NEWTASK07        	VARCHAR(3)                COMMENT '',
	NEWTASK08        	VARCHAR(3)                COMMENT '',
	OP_TYPE        		VARCHAR(1)                COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIPBES THE ORIGINAL OPERATION THE      ',
	OUTLET        		INT                       COMMENT 'OUTLET NUMBER, 1-6      ',
	`PARTITION`      	VARCHAR(2)                COMMENT 'DATA HASHING VALUE FOR DATABASE MANAGEMENT - EQUALS THE LAST 2 DIGITS OF THE HOUSE #    ',
	PIN        			VARCHAR(4)                COMMENT 'PERSONAL IDENTIFICATION NUMBER FOR PPV      ',
	PPV        			VARCHAR(1)                COMMENT 'PPV ENABLE FLAG     ',
	PTS        			INT                       COMMENT 'QUOTA POINTS FOR THJOB   ',
	RATING        		VARCHAR(1)                COMMENT 'EVENTS RATING FLAG OR PERCENTAGE OF TV HOUSEHOLDS VIEWING AT AN INTERVAL TIME, G,P,R,X OR 0-100     ',
	SERL        		VARCHAR(14)               COMMENT 'BOX SERIAL NUMBER ON THOUTLET    ',
	TASK01        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TASK02        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TASK03        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TASK04        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TASK05        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TASK06        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TASK07        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TASK08        		VARCHAR(2)                COMMENT 'TASK/PROBLEM CODE DEPENDING ON JOB TYPE, CODE95 TABLE 201,202,203   ',
	TSKCT01        		VARCHAR(2)                COMMENT 'ITEM COUNT FOR TASK COUNT 11  ',
	TSKCT02        		VARCHAR(2)                COMMENT 'ITEM COUNT FOR TASK COUNT     ',
	TSKCT03        		VARCHAR(2)                COMMENT 'ITEM COUNT FOR TASK COUNT 33  ',
	TSKCT04        		VARCHAR(2)                COMMENT 'ITEM COUNT FOR TASK COUNT 44  ',
	TSKCT05        		VARCHAR(2)                COMMENT 'ITEM COUNT FOR TASK COUNT 55  ',
	WIPOUTLET_SEQ       BIGINT                  	  COMMENT '',
	WPCNT               VARCHAR(1)                COMMENT 'A COUNTER TO LINK WIP RECORDS.A WPCNT OF 5 WOULD LINK ALL RECORDS WITH WPCNT 5 TO ONE TRANSACTN, THE NEXT TRANSACTION WOULD HAVE WPCNT 6 AND SO FORTH. LINKS THE HOUSE, WIP, WIP COMMENT     '
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