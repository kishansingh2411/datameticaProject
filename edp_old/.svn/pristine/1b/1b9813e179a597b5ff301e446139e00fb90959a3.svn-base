--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_auxhouse_tbl)
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

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(    
  CORP          INT           COMMENT '',
  `PARTITION` VARCHAR(2)    COMMENT '',
  HOUSE         VARCHAR(6)    COMMENT '',
  X1            VARCHAR(1)    COMMENT '',
  X2            VARCHAR(2)    COMMENT '',
  X3            VARCHAR(3)    COMMENT '',
  Y1            VARCHAR(1)    COMMENT '',
  Y2            VARCHAR(2)    COMMENT '',
  Y3            VARCHAR(3)    COMMENT '',
  Y6            VARCHAR(6)    COMMENT '',
  Y7            VARCHAR(7)    COMMENT '',
  MACHINE_ID    VARCHAR(5)    COMMENT '',
  OP_TYPE       VARCHAR(1)    COMMENT '',
  LOAD_DATE     TIMESTAMP     COMMENT '',
  AUXHOUSE_SEQ  BIGINT           COMMENT '',
  LOAD_YEAR     STRING,
  LOAD_MONTH    STRING
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################