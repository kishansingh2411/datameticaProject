--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_code36_tbl)
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


CREATE  TABLE CODE36_TMP
(    
  CORP        INT           COMMENT ' ',
  TABL        VARCHAR(3)    COMMENT ' ',
  CODE        VARCHAR(1)    COMMENT ' ',
  LONGDESC    VARCHAR(20)   COMMENT ' ',
  SHORTDES    VARCHAR(8)    COMMENT ' ',
  SORTFLAG    VARCHAR(1)    COMMENT ' ',
  MSOCODE     VARCHAR(1)    COMMENT ' ',
  MACHINE_ID  VARCHAR(5)    COMMENT ' ',
  OP_TYPE     VARCHAR(1)    COMMENT ' ',
  LOAD_DATE   TIMESTAMP     COMMENT ' ',
  CODE36_SEQ  BIGINT        COMMENT ' ',
  LOAD_YEAR   VARCHAR(4)    COMMENT ' ',
  LOAD_MONTH  VARCHAR(2)    COMMENT ' '
)
;

--##############################################################################
--#                                    End                                     #
--##############################################################################