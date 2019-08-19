--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(work_rsdvr_recording_archive_tbl)
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
    ID_SCHEDULE             BIGINT      COMMENT '',
    ID_ASSET                BIGINT      COMMENT '',
    CALLSIGN                VARCHAR(12) COMMENT '',
    STARTDATE               TIMESTAMP   COMMENT '',
    STARTTIME               INT         COMMENT '',
    DURATION                INT         COMMENT '',
    REC_STATUS              INT         COMMENT '',
    CREATE_TIME             TIMESTAMP   COMMENT '',
    MODIFY_TIME             TIMESTAMP   COMMENT '',
    REC_LOCK                INT         COMMENT '',
    REC_DATE                TIMESTAMP   COMMENT '',
    REC_TIME                INT         COMMENT '',
    ID_RECORDING            BIGINT      COMMENT '',
    STARTDATE_STARTTIME     TIMESTAMP   COMMENT '',
    TEMPORARY_ID_ASSET      INT         COMMENT '',
    PAUSE_TIME              INT         COMMENT '',
    ENDTIME_ADJUSTMENT      INT         COMMENT '',
    ENDTIME_ADJUSTMENT_PREV INT         COMMENT '',
    KEEP_TYPE_CODE          INT         COMMENT '',
    ID_HOME                 BIGINT      COMMENT '',
    ARCHIVE_TIME            TIMESTAMP   COMMENT '',
    STARTDATE_STARTTIME_EST TIMESTAMP   COMMENT '',
    STARTDATE_EST           DATE        COMMENT '',
    ARCHIVE_DATE_EST        DATE        COMMENT '',
    SOURCE_LOAD_DATE        DATE        COMMENT ''
)
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################