--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_ratepricearea_tbl)
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
	CORP        		INT                COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP',
	RCODE        		VARCHAR(2)         COMMENT 'RATE CODE--TO CHARGE FOR A SERVICE OR COMBO OF SERVICES.  VALID VALUES:  ANY 2 ALPHA CHARACTERS.  AZ-AH ARE NON-STANDARD RATES AND CODES BEGINNING WITH X ARE FREE RATES',
	LEVELNUM        	INT                COMMENT 'LEVEL OF THIS MASS CORRECTIONS FORMAT, OR LEVEL FOR MSO DATA, OR RATE PRICE LEVEL.  VALID VALUES:  1-3 OR IF MSO S=STANDALONE, I=INTERNAL',
	AREASTOP        	INT                COMMENT 'END OF GEOGRAPHIC AREA TO WHICH THIS PRICE LEVEL APPLIES',
	AREASTART        	INT                COMMENT 'START OF GEOGRAPHIC AREA TO WHICH THIS PRICE LEVEL APPLIES',
	MACHINE_ID        	VARCHAR(5)         COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM',
	OP_TYPE        		VARCHAR(1)         COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIBES THE ORIGINAL OPERATION TYPE FOR THE RECORD WHEN IT WAS LOADED INTO THE CABLEDATA DATABASE',
	GGS_TRANS_TIMESTAMP VARCHAR(26)        COMMENT 'THE GOLDEN GATE ASSIGNED DATE OF WHEN THE RECORD WAS LAST UPDATED ON THE TANDEM',
	LOAD_DATE        	TIMESTAMP          COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE',
	RATEPRICEAREA_SEQ   BIGINT             COMMENT 'A UNIQUE, ASCENDING NUMBER ASSIGNED WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE'
	
)
PARTITIONED BY (LOAD_YEAR STRING,
                LOAD_MONTH STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
; 

--##############################################################################
--#                                    End                                     #
--##############################################################################