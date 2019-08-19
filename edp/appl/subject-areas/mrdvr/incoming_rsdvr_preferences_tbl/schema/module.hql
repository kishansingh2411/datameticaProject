--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_rsdvr_preferences_tbl)
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
    ID_HOME                VARCHAR(255)	  COMMENT 'home id of the requesting box',
    MACADDRESS             VARCHAR(255)   COMMENT 'macaddress of the requesting box',
    SERIALNUM              VARCHAR(255)   COMMENT 'serial number of the requesting box',
    SORT_OPTION            DECIMAL(1,0)   COMMENT 'the users preference for list sorting',
    SPACE_ALLOTTED         BIGINT 		  COMMENT 'the space alotted in the home (in KB)',
    SPACE_USED             BIGINT 		  COMMENT 'the space used in the home (in KB)',
    AUTO_ERASE             VARCHAR(1)     COMMENT 'should the recordings be erased when space is needed',
    SAVE_DAYS              DECIMAL(5,0)   COMMENT 'always 360 - not yet supported',
    NODE_ID                DECIMAL(5,0)   COMMENT 'the service group of the home',
    BLOCK_TITLES           VARCHAR(1)     COMMENT 'whether or not blocked titles is turned on for the home',
    STATUS                 INT            COMMENT 'The status of the provisioning operation for this home',
    CREATE_TIME            TIMESTAMP      COMMENT '',
    STATUS_CHANGED         TIMESTAMP      COMMENT '',
    INGESTS_ALLOTED        INT			  COMMENT '',
    VERSION1               VARCHAR(2)     COMMENT '',
    REC_PAST_FOLDERS       VARCHAR(1)     COMMENT 'Y to see the recorded list in folders',
    REC_EPISODE_KEEP       VARCHAR(4)     COMMENT 'SN Space needed XD until deleted 1-14 - # days',
    REC_EPISODE_QUALITY    VARCHAR(1)     COMMENT 'H = HD, S = SD',
    REC_EPISODE_STOP       VARCHAR(8)     COMMENT 'minutes before (neg) or after (pos) the scheduled endtime the user wants the recording to end',
    REC_SERIES_KEEP        VARCHAR(8)     COMMENT 'SN Space needed XD until deleted 1-14 - # days',
    REC_SERIES_QUALITY     VARCHAR(4)     COMMENT 'H = HD, S = SD',
    REC_SERIES_STOP        VARCHAR(8)     COMMENT 'minutes before (neg) or after (pos) the scheduled endtime the user wants the recording to end',
    REC_SERIES_SAVE_NUM_EP VARCHAR(6)     COMMENT 'the number of episodes of a series to save (0 - all)',
    REC_SERIES_TYPE        VARCHAR(2)     COMMENT 'N - new only, T - any Timeslot, A - Any Time',
    RECORDER_ID            INT            COMMENT '',
    CREATED_BY             VARCHAR(64)    COMMENT '',
    MODIFY_TIME            TIMESTAMP      COMMENT '',
    MODIFIED_BY            VARCHAR(64)    COMMENT '',
    SYST                   VARCHAR(6)     COMMENT '',
	LOADTIME          	   TIMESTAMP      COMMENT 'date/time data loaded into table',
	LAST_UPDATED           TIMESTAMP      COMMENT ''
)
PARTITIONED BY (LOAD_DATE STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################