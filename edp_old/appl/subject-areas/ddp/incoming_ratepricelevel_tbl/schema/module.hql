--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_ratepricelevel_tbl)
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
	CORP        		INT                COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP.',
	RCODE        		VARCHAR(2)         COMMENT 'RATE CODE--TO CHARGE FOR A SERVICE OR COMBO OF SERVICES.  VALID VALUES:  ANY 2 ALPHA CHARACTERS.  AZ-AH ARE NON-STANDARD RATES AND CODES BEGINNING WITH X ARE FREE RATES.',
	LEVELNUM        	INT                COMMENT 'LEVEL OF THMASS CORRECTIONS FORMAT, OR LEVEL FOR MSO DATA, OR RATE PRICE LEVEL.  VALID VALUES:  1-3 OR IF MSO S=STANDALONE, I=INTERNAL',
	PRICENAME        	VARCHAR(16)        COMMENT 'PRICE LEVEL LONG DESCRIPTION.  VALID VALUES:  FREE-FORM TEXT',
	PRICEDESC        	VARCHAR(8)         COMMENT 'PRICE LEVEL SHORT DESCRIPTION.  VALID VALUES:  FREE-FORM TEXT',
	RAMT        		DECIMAL(6,2)       COMMENT 'DOLLAR AMOUNT TO CHARGE FOR THRATE CODE',
	PRIORAMT        	DECIMAL(6,2)       COMMENT 'THE DOLLAR AMOUNT FOR THRATE CODE PRIOR TO THE NEW RATE BEING PUT INTO RAMT',
	EFFDATE        		DATE               COMMENT 'THE EFFECTIVE DATE FOR A RATE CHANGE ON THRATE CODE OR MSO TABLES ARC EFFECTIVE DATE',
	ACTVONCYC       	VARCHAR(1)         COMMENT 'FLAGS WHEN PRORATE RUN EFFECTIVE.  VALID VALUES:  Y=PRORATE RUN EFFECTIVE ON CYCLE, N=PRORATE RUNN EFFECTIVE ON DATE PRORATE RUN WAS RUN',
	PRORATEONCYC        VARCHAR(1)         COMMENT 'FLAG THAT INDICATES HOW TO HANDLE A CUSTOMER WHO HAD APRORATE WITH THRATE CODE DURING A R/I.  VALID VALUES:  Y=PRORATE AT OLD AMOUNT, N=CHARGE NEW RATE IN PRORATE FROM R/I EFF. DATE',
	CAMPG        		VARCHAR(1)         COMMENT 'CAMPAIGN CODE FOR THE INSTALLATION',
	MACHINE_ID        	VARCHAR(5)         COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.',
	OP_TYPE        		VARCHAR(1)         COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIBES THE ORIGINAL OPERATION TYPE FOR THE RECORD WHEN IT WAS LOADED INTO THE CABLEDATA DATABASE',
	GGS_TRANS_TIMESTAMP VARCHAR(26)        COMMENT 'THE GOLDEN GATE ASSIGNED DATE OF WHEN THE RECORD WAS LAST UPDATED ON THE TANDEM',
	LOAD_DATE        	TIMESTAMP          COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE',
	RATEPRICELEVEL_SEQ  BIGINT             COMMENT 'A UNIQUE, ASCENDING NUMBER ASSIGNED WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE'
	
)
PARTITIONED BY (LOAD_YEAR STRING,
                LOAD_MONTH STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
; 

--##############################################################################
--#                                    End                                     #
--##############################################################################