--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_rsdvr_requests_archive_tbl)
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
    ID_HOME                 VARCHAR(255)      COMMENT '',
    SERIALNUM               VARCHAR(255) COMMENT '',
    MACADDRESS              VARCHAR(255) COMMENT '',
    CALLSIGN                VARCHAR(12) COMMENT '',
    STARTDATE               TIMESTAMP   COMMENT '',
    STARTTIME               INT         COMMENT '',
    DURATION                INT         COMMENT '',
    REC_TYPE                INT         COMMENT '',
    SAVE_ALL                INT         COMMENT '',
    SAVE_LATEST             INT         COMMENT '',
    CREATE_TIME             TIMESTAMP   COMMENT '',
    MODIFY_TIME             TIMESTAMP   COMMENT '',
    ENDTIME_ADJUSTMENT      INT         COMMENT '',
    SAVE_DAYS               INT         COMMENT '',
    STATUS                  INT         COMMENT '',
    SHORT_TITLE             VARCHAR(64) COMMENT '',
    ARCHIVE_TIME            TIMESTAMP   COMMENT '',
    ARCHIVE_DATE_EST        DATE        COMMENT '',
    SOURCE_LOAD_DATE        DATE        COMMENT ''
)

PARTITIONED BY (LOAD_DATE STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################