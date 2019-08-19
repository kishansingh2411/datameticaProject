--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_rsdvr_requests_archive_dedup_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 05/05/2016
--#   Log File    : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/mrdvr/MRDVR_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/05/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(  
	SCHEDULE_ID_INCOMING                    BIGINT            COMMENT'',
     CALL_SIGN_INCOMING                             VARCHAR(12)   COMMENT'',
     DURATION_INCOMING                              INT               COMMENT'',
     END_TIME_ADJUSTMENT_INCOMING   INT               COMMENT'',
     HOME_ID_INCOMING                               VARCHAR(255)  COMMENT'',
     MAC_ADDRESS_INCOMING                   VARCHAR(255)  COMMENT'',
     SERIAL_NUMBER_INCOMING                         VARCHAR(255)  COMMENT'',
     CREATE_TIME_INCOMING                   TIMESTAMP         COMMENT'',
     MODIFIED_TIME_INCOMING                         TIMESTAMP         COMMENT'',
     RECORDING_TYPE_INCOMING                INT               COMMENT'',
     SAVE_ALL_INCOMING                              INT           COMMENT'',
     SAVE_LATEST_INCOMING                   INT           COMMENT'',
     START_DATE_INCOMING                    TIMESTAMP     COMMENT'',
     START_TIME_INCOMING                    INT           COMMENT'',
     SAVE_DAYS_INCOMING               INT          COMMENT'',
     STATUS_INCOMING                  INT          COMMENT'',
     SHORT_TITLE_INCOMING             VARCHAR(64)  COMMENT'',
     ARCHIVE_TIME_INCOMING            TIMESTAMP    COMMENT'',
     ARCHIVE_DATE_EST_INCOMING        DATE         COMMENT'',
     SOURCE_LOAD_DATE_INCOMING                 DATE         COMMENT'',
    SCHEDULE_ID_GOLD                                BIGINT            COMMENT'',
    CALL_SIGN_GOLD                                  VARCHAR(12)   COMMENT'',
    DURATION_GOLD                                   INT               COMMENT'',
    END_TIME_ADJUSTMENT_GOLD                INT               COMMENT'',
    HOME_ID_GOLD                                    VARCHAR(255)  COMMENT'',
    MAC_ADDRESS_GOLD                                VARCHAR(255)  COMMENT'',
    CREATE_TIME_GOLD                                TIMESTAMP     COMMENT'',
    MODIFIED_TIME_GOLD                              TIMESTAMP     COMMENT'',
    RECORDING_TYPE_GOLD                     INT         COMMENT'',
    SAVE_ALL_GOLD                                   INT               COMMENT'',
    SAVE_LATEST_GOLD                                INT               COMMENT'',
    SERIAL_NUMBER_GOLD                              VARCHAR(255)  COMMENT'',
    START_DATE_GOLD                                 TIMESTAMP     COMMENT'',
    START_TIME_GOLD                                 VARCHAR(8)    COMMENT'',
    SAVE_DAYS_GOLD     DECIMAL(5, 0),
    STATUS_GOLD                        BIGINT,
    SHORT_TITLE_GOLD                  VARCHAR(128),
    ARCHIVE_DATE_EST_GOLD             DATE,
    SOURCE_LOAD_DATE_GOLD                  DATE,
    OP_TYPE_GOLD                                    VARCHAR(1)        COMMENT'',
    MAC_ADDRESS_IND                      CHAR(1)           COMMENT '',
    CALL_SIGN_IND                        CHAR(1)           COMMENT '',
    START_DATE_IND                       CHAR(1)           COMMENT '',
    START_TIME_IND                       CHAR(1)           COMMENT '',
    DURATION_IND                         CHAR(1)           COMMENT '',
    RECORDING_TYPE_IND                   CHAR(1)           COMMENT '',
    SAVE_ALL_IND                         CHAR(1)           COMMENT '',
    SAVE_LATEST_IND                      CHAR(1)           COMMENT '',
    CREATE_TIME_IND                 CHAR(1)    COMMENT'',
    MODIFIED_TIME_IND                    CHAR(1)           COMMENT '',
    END_TIME_ADJUSTMENT_IND              CHAR(1)           COMMENT '',
    SAVE_DAYS_IND                 CHAR(1)           COMMENT'',
    STATUS_IND                    CHAR(1)           COMMENT'',
    SHORT_TITLE_IND               CHAR(1)           COMMENT'',
    ARCHIVE_DATE_EST_IND          CHAR(1)           COMMENT'',
    SOURCE_LOAD_DATE_IND          CHAR(1)           COMMENT'',
    NEWFLAG                                                 VARCHAR(10)   COMMENT'',
    CHANGEDFLAG                                     VARCHAR(10)   COMMENT'',
    LOAD_DATE                                               TIMESTAMP        COMMENT'',
    DTM_SYSDATE                                     TIMESTAMP     COMMENT'',
    Y_IND                                                   VARCHAR(1)    COMMENT'',
    OP_TYPE_I 						VARCHAR(1)    COMMENT'',
    OP_TYPE_U 						VARCHAR(1)    COMMENT'',
	RSDVR_REQUESTS_ARCHIVE_DELTA_SEQ        BIGINT        COMMENT''
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################
