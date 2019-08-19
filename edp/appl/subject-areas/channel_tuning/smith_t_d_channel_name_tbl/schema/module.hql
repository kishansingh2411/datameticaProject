--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(gold_t_d_channel_name_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : ...log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/30/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   CHANNEL_NAME_ID               INT           COMMENT 'Unique identification of this table within the Audience Measurement Database Application.  This is a system assigned non-intrinsic number.',
   CHANNEL_NAME                  VARCHAR(63)   COMMENT 'Natural data used to uniquely identify a row in this table.  aka: Call Sign.  the characters used to define a unique row may include spaces.  This is sourced from several sources (i.e. trigger aggregator from the set-top box channel tuning aggregator from the set-top box EPG (Electronic Programming Guide) sourced from eMedia.',
   SOURCED_FROM_SYSTEM           VARCHAR(31)   COMMENT 'generally accepted name of the system the data was sourced from.  hard coded.',
   LAST_UPDATED_BY_SYSTEM        VARCHAR(31)   COMMENT 'generally accepted name of the system the data was last updated.  hard coded.  Initially set to NULL on an insert.',
   DTM_CREATED                   TIMESTAMP     COMMENT '',
   DTM_LAST_UPDATED              TIMESTAMP     COMMENT 'Date and time when the record was last updated.  This column is NULL when the record is inserted.',
   INS_STB_FILE_CONTROL_ID       INT           COMMENT 'Foreign key to D_STB_FTP_FILE_CONTROL table.  Identification of the source directory and file used to insert this record.',
   UPD_STB_FILE_CONTROL_ID       INT           COMMENT '',
   TMS_NETWORK_ID                INT           COMMENT '',
   TMS_NETWORK_NBR               INT           COMMENT '',
   TMS_NETWORK_TIME_ZONE         VARCHAR(45)   COMMENT '',
   TMS_NETWORK_NAME              VARCHAR(40)   COMMENT '',
   TMS_FCC_CALL_SIGN             VARCHAR(10)   COMMENT '',
   TMS_NETWORK_AFFILIATE         VARCHAR(25)   COMMENT '',
   TMS_NETWORK_CITY              VARCHAR(20)   COMMENT 'Network mai city. Sourced from CDCUSR.STATREC_LAST_REC.NETWORK_CITY.  2008.09.08 J.McKenna/D.Patel default value "Unknown"',
   TMS_NETWORK_STATE             VARCHAR(15)   COMMENT '',
   TMS_NETWORK_ZIP_CODE          VARCHAR(12)   COMMENT '',
   TMS_NETWORK_COUNTRY           VARCHAR(15)   COMMENT '',
   TMS_DESIGNATES_MARKET_NAME    VARCHAR(70)   COMMENT '',
   TMS_DESIGNATES_MARKET_NBR     VARCHAR(10)   COMMENT '',
   TMS_FCC_CHANNEL_NBR           VARCHAR(8)    COMMENT '',
   TMS_NETWORK_LANGUAGE          VARCHAR(40)   COMMENT '',
   CHANNEL_DESCR                 VARCHAR(100)  COMMENT '',
   CVC_CALL_SIGN                 VARCHAR(63)   COMMENT '',
   PEG_IND                       CHAR(1)       COMMENT '',
   CHANNEL_CATEGORY              VARCHAR(25)   COMMENT '',
   WOODBURY_LINEUP_CHANNEL_NBR   INT           COMMENT '',
   PARENT_NETWORK                VARCHAR(64)   COMMENT '',
   NETWORK_GROUP                 VARCHAR(64)   COMMENT '',
   NETWORK_ALIAS                 VARCHAR(64)   COMMENT '',
   PROGRAM_EXCLUSION_IND         CHAR(1)       COMMENT '',
   INSERTABLE_IND                CHAR(1)       COMMENT '',
   BROADCAST_FLAG                CHAR(1)       COMMENT '',
   STRATA_NAME                   VARCHAR(250)  COMMENT '',
   TGT_DTM_CREATED               TIMESTAMP     COMMENT '',
   TGT_DTM_LAST_UPDATED          TIMESTAMP     COMMENT '',
   STRATA_STATION_ID             INT           COMMENT ''
)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################