--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_custbilladdr_tbl)
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

CREATE  TABLE CUSTBILLADDR_TMP
(
    BIL1              VARCHAR(26)        COMMENT 'The is in care ofis  line ',
    BIL2              VARCHAR(26)        COMMENT 'Street address',
    BIL3              VARCHAR(17)        COMMENT 'City and state',
    BIL4              VARCHAR(6)         COMMENT 'Billing zip code',
	CORP              INT                COMMENT 'Five digit system number assigned to each corp.',
	CUST              VARCHAR(2)         COMMENT 'Customer number. Part of the Customer account number.  Refer to Appendix_doc #2.',
	CUSTBILLADDR_SEQ  BIGINT             COMMENT '',
	HOUSE             VARCHAR(6)         COMMENT 'House number, Account Number, Alpha/Numeric, Refer to Apendix_doc #2',    
    LOAD_DATE         TIMESTAMP          COMMENT 'Oracle assigned date of when the record was loaded into the Oracle Database',
    MACHINE_ID        VARCHAR(5)         COMMENT 'The id of the tandem that the original record was extracted from.',
    OP_TYPE           VARCHAR(1)         COMMENT 'Golden Gate Assigned operation type that descripbes the original operation the',
    `PARTITION`       VARCHAR(22)	     COMMENT 'Data hashing value for database management - equals the last 2 digits of the house #',    
    LOAD_YEAR         VARCHAR(4)         COMMENT '',
    LOAD_MONTH        VARCHAR(2)         COMMENT ''
)
;

--##############################################################################
--#                                    End                                     #
--##############################################################################