--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_raterptctrs_tbl)
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

CREATE TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name} 
 ( 
	CORP      	  		INT       		          COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP.',
	DRATECODE     		VARCHAR(2)                COMMENT 'RATE CODE',
	RCTR01        		VARCHAR(2)                COMMENT 'REPORTING CENTER ONE FOR THIS RATE',
	RCTR02        		VARCHAR(2)                COMMENT 'REPORTING CENTER TWO FOR THIS RATE',
	RCTR03        		VARCHAR(2)                COMMENT 'REPORTING CENTER THREE FOR THIS RATE',
	RCTR04        		VARCHAR(2)                COMMENT 'REPORTING CENTER FOUR FOR THIS RATE',
	RCTR05        		VARCHAR(2)                COMMENT 'REPORTING CENTER FIVE FOR THIS RATE',
	RCTR06        		VARCHAR(2)                COMMENT 'REPORTING CENTER SIX FOR THIS RATE',
	RCTR07        		VARCHAR(2)                COMMENT 'REPORTING CENTER SEVEN FOR THIS RATE',
	RCTR08        		VARCHAR(2)                COMMENT 'REPORTING CENTER EIGHT FOR THIS RATE',
	RCTR09        		VARCHAR(2)                COMMENT 'REPORTING CENTER NINE FOR THIS RATE',
	RCTR10        		VARCHAR(2)                COMMENT 'REPORTING CENTER TEN FOR THIS RATE',
	RCTR11        		VARCHAR(2)                COMMENT 'REPORTING CENTER ELEVEN FOR THIS RATE',
	RCTR12        		VARCHAR(2)                COMMENT 'REPORTING CENTER TWELVE FOR THIS RATE',
	RCTR13        		VARCHAR(2)                COMMENT 'REPORTING CENTER THIRTEEN FOR THIS RATE',
	RCTR14        		VARCHAR(2)                COMMENT 'REPORTING CENTER FOURTEEN FOR THIS RATE',
	RCTR15        		VARCHAR(2)                COMMENT 'REPORTING CENTER FIFTEEN FOR THIS RATE',
	RCTR16        		VARCHAR(2)                COMMENT 'REPORTING CENTER SIXTEEN FOR THIS RATE',
	RCTR17        		VARCHAR(2)                COMMENT 'REPORTING CENTER SEVENTEEN FOR THIS RATE',
	RCTR18        		VARCHAR(2)                COMMENT 'REPORTING CENTER EIGHTEEN FOR THIS RATE',
	RCTR19        		VARCHAR(2)                COMMENT 'REPORTING CENTER NINETEEN FOR THIS RATE',
	RCTR20        		VARCHAR(2)                COMMENT 'REPORTING CENTER TWENTY FOR THIS RATE',
	MACHINE_ID    		VARCHAR(5)                COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.',
	OP_TYPE       		VARCHAR(1)                COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIPBES THE ORIGINAL OPERATION THE',
	LOAD_DATE     		TIMESTAMP       		  COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE',
	RATERPTCTRS_SEQ     BIGINT             		  COMMENT '',
	GGS_TRANS_TIMESTAMP VARCHAR(26)               COMMENT 'GOLDEN GATE ASSIGNED DATE FOR WHICH THE TRANSACTION OCCURRED ON THE TANDEM',
	LOAD_YEAR           STRING,
    LOAD_MONTH          STRING
)
STORED AS AVRO
LOCATION '${hivevar:location}'
; 


--##############################################################################
--#                                    End                                     #
--##############################################################################