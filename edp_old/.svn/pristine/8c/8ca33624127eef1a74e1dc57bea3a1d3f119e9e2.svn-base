--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_slsmn_tbl)
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

CREATE  TABLE SLSMN_TMP
( 
	CORP              INT                COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP',
	SALESMAN          INT                COMMENT ' ',
	SMANLAST          VARCHAR(12)        COMMENT ' ',
	SMANINIT          VARCHAR(2)         COMMENT ' ',
	SMANSTAT          VARCHAR(1)         COMMENT ' ',
	SMANTYPE          VARCHAR(1)         COMMENT ' ',
	SMANCOMT          VARCHAR(70)        COMMENT ' ',
	MACHINE_ID        VARCHAR(5)         COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.',
	OP_TYPE           VARCHAR(1)         COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIPBES THE ORIGINAL OPERATION THE',
	LOAD_DATE         TIMESTAMP          COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE',
	SLSMN_SEQ         BIGINT             COMMENT ' ',
	SALESREP          VARCHAR(3)         COMMENT ' ',
    LOAD_YEAR         VARCHAR(4)         COMMENT ' ',
    LOAD_MONTH        VARCHAR(2)       	 COMMENT ' '
)
;

--##############################################################################
--#                                    End                                     #
--##############################################################################