--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_boxinvtry_tbl)
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
  CORP                 INT            COMMENT ' ',
  BNUMB                VARCHAR(14)    COMMENT ' ',
  ADDLEQ               VARCHAR(8)     COMMENT ' ',
  BEQUIP               VARCHAR(1)     COMMENT ' ',
  MFG                  VARCHAR(1)     COMMENT ' ',
  LOT                  VARCHAR(3)     COMMENT ' ',
  PURCHDATE            DATE           COMMENT ' ',
  COST                 DECIMAL(5,2)   COMMENT ' ',
  PREVHOUSE            VARCHAR(6)     COMMENT ' ',
  PREVCUST             VARCHAR(2)     COMMENT ' ',
  OPTN1                VARCHAR(1)     COMMENT ' ',
  OPTN2                VARCHAR(1)     COMMENT ' ',
  OPTN3                VARCHAR(1)     COMMENT ' ',
  TECH                 INT            COMMENT ' ',
  LOC                  VARCHAR(1)     COMMENT ' ',
  PLACE                VARCHAR(4)     COMMENT ' ',
  RECSTAT              VARCHAR(1)     COMMENT ' ',
  BXSTATUS             VARCHAR(1)     COMMENT ' ',
  ADDRSABLE            VARCHAR(1)     COMMENT ' ',
  EVTCPBL              VARCHAR(1)     COMMENT ' ',
  NUMREPAIRS           INT            COMMENT ' ',
  LASTDATE             DATE           COMMENT ' ',
  LASTPROG             VARCHAR(1)     COMMENT ' ',
  BRGPIN               VARCHAR(4)     COMMENT ' ',
  BRGRATG              VARCHAR(1)     COMMENT ' ',
  BRGPPV               VARCHAR(1)     COMMENT ' ',
  BRGEMG               VARCHAR(1)     COMMENT ' ',
  CABLE                VARCHAR(1)     COMMENT ' ',
  FMT                  VARCHAR(1)     COMMENT ' ',
  TIERS                VARCHAR(16)    COMMENT ' ',
  SPCLS                VARCHAR(8)     COMMENT ' ',
  PROM                 INT            COMMENT ' ',
  HUB                  VARCHAR(2)     COMMENT ' ',
  TINITFLAG            VARCHAR(1)     COMMENT ' ',
  NORESP               VARCHAR(1)     COMMENT ' ',
  DELDATE              DATE           COMMENT ' ',
  DWO                  VARCHAR(1)     COMMENT ' ',
  HIBIT                INT            COMMENT ' ',
  OUTLET               INT            COMMENT ' ',
  HOUSE                VARCHAR(6)     COMMENT ' ',
  CUST                 VARCHAR(2)     COMMENT ' ',
  WPCNT                VARCHAR(1)     COMMENT ' ',
  OPAID                VARCHAR(14)    COMMENT ' ',
  OPALOGPORT           INT            COMMENT ' ',
  OPADROP              VARCHAR(1)     COMMENT ' ',
  OPASUBMOD            VARCHAR(1)     COMMENT ' ',
  `PARTITION`          VARCHAR(2)     COMMENT ' ',
  OPR                  VARCHAR(3)     COMMENT ' ',
  BOXEQUIP             VARCHAR(2)     COMMENT ' ',
  NVODCAPABLE          VARCHAR(1)     COMMENT ' ',
  IPPVPIN              VARCHAR(5)     COMMENT ' ',
  FEATUREMAP1          INT            COMMENT ' ',
  FEATUREMAP2          INT            COMMENT ' ',
  SUB_TYPE             INT            COMMENT ' ',
  UNIT_ADD             VARCHAR(16)    COMMENT ' ',
  PREVTIERS            VARCHAR(16)    COMMENT ' ',
  POLLING_DATE         DATE           COMMENT ' ',
  CONFIG_ID_NUM        INT            COMMENT ' ',
  MACHINE_ID           VARCHAR(5)     COMMENT ' ',
  OP_TYPE              VARCHAR(1)     COMMENT ' ',
  LOAD_DATE            TIMESTAMP      COMMENT ' ',
  T_TIMESTAMP          DATE           COMMENT ' ',
  BOXINVTRY_SEQ        BIGINT            COMMENT ' ',
  DELIVERYSYSID        VARCHAR(20)    COMMENT ' ',
  EQUIPMENT_CLASS      INT            COMMENT ' ',
  EQUIP_VERSION        VARCHAR(5)     COMMENT ' ',
  HIGHPRI_REMOVE       DATE           COMMENT ' ',
  IPADDR               VARCHAR(15)    COMMENT ' ',
  MACADDRESS           VARCHAR(17)    COMMENT ' ',
  MODEL                VARCHAR(30)    COMMENT ' ',
  MODEMID              VARCHAR(30)    COMMENT ' ',
  SUBSCRIBEROWNED_FLG  INT            COMMENT ' ',
  EXPANDED_TIERS       VARCHAR(109)   COMMENT ' ',
  ADDRESS_TYPE         VARCHAR(2)     COMMENT ' ',
  MDUFLAG              INT            COMMENT ' ',
  MDULOCATION_COMMENT  VARCHAR(25)    COMMENT ' '
)
PARTITIONED BY (LOAD_YEAR STRING,
                LOAD_MONTH STRING, 
                LOAD_DAY STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################