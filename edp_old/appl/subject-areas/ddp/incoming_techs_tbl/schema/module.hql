--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_techs_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 05/02/2016
--#   Log File    : .../log/DEPLOYMENT_${current_timestamp}.log
--#   SQL File    : 
--#   Error File  : .../log/DEPLOYMENT_${current_timestamp}.log
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
	CORP      	  INT        	    COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP',
	TECH      	  INT        		COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE',
	TECHLAST  	  VARCHAR(13)		COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.',
	TECHINIT  	  VARCHAR(2) 		COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIPBES THE ORIGINAL OPERATION THE',
	TECHSTAT  	  VARCHAR(1) 		COMMENT ' ',
	TECHTYPE  	  VARCHAR(1) 		COMMENT ' ',
	TECHSKILL 	  VARCHAR(1) 		COMMENT ' ',
	TECHPRT   	  VARCHAR(1) 		COMMENT ' ',
	TECHSCHED 	  VARCHAR(1) 		COMMENT ' ',
	TECHPTS   	  VARCHAR(2) 		COMMENT ' ',
	TECHTRKNUM	  VARCHAR(7) 		COMMENT ' ',
	TECHTRKTYP	  VARCHAR(10)		COMMENT ' ',
	TECHCOMT  	  VARCHAR(70)		COMMENT ' ',
	TECHBATCH 	  INT        		COMMENT ' ',
	MACHINE_ID	  VARCHAR(5) 		COMMENT ' ',
	OP_TYPE   	  VARCHAR(1) 		COMMENT ' ',
	LOAD_DATE 	  TIMESTAMP 		COMMENT ' ',
	TECHS_SEQ 	  BIGINT       		COMMENT ' ',
	OPR       	  VARCHAR(3)		COMMENT ' '	
)
PARTITIONED BY (LOAD_YEAR STRING,
                LOAD_MONTH STRING)
STORED AS AVRO
LOCATION '${hivevar:location}';
;

--##############################################################################
--#                                    End                                     #
--##############################################################################