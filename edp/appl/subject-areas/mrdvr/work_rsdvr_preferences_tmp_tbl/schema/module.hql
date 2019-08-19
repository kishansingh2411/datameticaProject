--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Work table(work_rsdvr_preferences_tmp_tbl)
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
	HOME_ID_INCOMING                       VARCHAR(255)   COMMENT 'home id of the requesting box',
	MAC_ADDRESS_INCOMING                   VARCHAR(255)   COMMENT 'macaddress of the requesting box',
	SERIAL_NUMBER_INCOMING                 VARCHAR(255)   COMMENT 'serial number of the requesting box',
	SORT_OPTION_INCOMING                   INT            COMMENT 'the users preference for list sorting',
	SPACE_ALLOTED_INCOMING                 BIGINT         COMMENT 'the space alotted in the home (in KB)',
	SPACE_USED_INCOMING                    BIGINT         COMMENT 'the space used in the home (in KB)',
	AUTO_ERASE_INCOMING                    VARCHAR(1)     COMMENT 'should the recordings be erased when space is needed',
	SAVE_DAYS_INCOMING                     INT            COMMENT 'always 360 - not yet supported',
	NODE_ID_INCOMING                       INT            COMMENT 'the service group of the home',
	BLOCK_TITLES_INCOMING                  VARCHAR(5)     COMMENT 'whether or not blocked titles is turned on for the home',
	STATUS_INCOMING                        INT            COMMENT 'The status of the provisioning operation for this home',
	CREATE_TIME_INCOMING                   TIMESTAMP      COMMENT '',
	STATUS_CHANGED_INCOMING                TIMESTAMP      COMMENT '',
	INGESTS_ALLOTED_INCOMING               INT            COMMENT '',
	VERSION1_INCOMING                      VARCHAR(2)     COMMENT '',
    REC_PAST_FOLDERS_INCOMING              VARCHAR(1)     COMMENT 'Y to see the recorded list in folders',
    REC_EPISODE_KEEP_INCOMING              VARCHAR(4)     COMMENT 'SN Space needed XD until deleted 1-14 - # days',
    REC_EPISODE_QUALITY_INCOMING           VARCHAR(1)     COMMENT 'H = HD, S = SD',
    REC_EPISODE_STOP_INCOMING              VARCHAR(8)     COMMENT 'minutes before (neg) or after (pos) the scheduled endtime the user wants the recording to end',
    REC_SERIES_KEEP_INCOMING               VARCHAR(8)     COMMENT 'SN Space needed XD until deleted 1-14 - # days',
    REC_SERIES_QUALITY_INCOMING            VARCHAR(4)     COMMENT 'H = HD, S = SD',
    REC_SERIES_STOP_INCOMING               VARCHAR(8)     COMMENT 'minutes before (neg) or after (pos) the scheduled endtime the user wants the recording to end',
    REC_SERIES_SAVE_NUM_EP_INCOMING        VARCHAR(6)     COMMENT 'the number of episodes of a series to save (0 - all)',
    REC_SERIES_TYPE_INCOMING               VARCHAR(2)     COMMENT 'N - new only, T - any Timeslot, A - Any Time',
    RECORDER_ID_INCOMING                   INT            COMMENT '',
    CREATED_BY_INCOMING                    VARCHAR(64)    COMMENT '',
    MODIFY_TIME_INCOMING                   TIMESTAMP      COMMENT '',
    MODIFIED_BY_INCOMING                   VARCHAR(64)    COMMENT '',
    SYST_INCOMING                          VARCHAR(6)     COMMENT '',
	LOADTIME_INCOMING          	           TIMESTAMP      COMMENT 'date/time data loaded into table',
	LAST_UPDATED_INCOMING                  TIMESTAMP      COMMENT '',
	HOME_ID_GOLD                           VARCHAR(255)   COMMENT '', 
	MAC_ADDRESS_GOLD                       VARCHAR(255)   COMMENT '',
	SERIAL_NUMBER_GOLD                     VARCHAR(255)   COMMENT '',
	SORT_OPTION_GOLD                       INT            COMMENT '',
	SPACE_ALLOTED_GOLD                     BIGINT         COMMENT '',
	SPACE_USED_GOLD                        BIGINT         COMMENT '',
	AUTO_ERASE_GOLD                        VARCHAR(1)     COMMENT '',
	SAVE_DAYS_GOLD                         INT            COMMENT '',
	NODE_ID_GOLD                           INT            COMMENT '',
	BLOCK_TITLES_GOLD                      VARCHAR(5)     COMMENT '',
	STATUS_GOLD                            INT            COMMENT '',
	CREATE_TIME_GOLD                       TIMESTAMP      COMMENT '',
	STATUS_CHANGED_GOLD                    TIMESTAMP      COMMENT '',
	INGESTS_ALLOTED_GOLD                   INT            COMMENT '',
	VERSION1_GOLD                          VARCHAR(2)     COMMENT '',
    REC_PAST_FOLDERS_GOLD                  VARCHAR(1)     COMMENT 'Y to see the recorded list in folders',
    REC_EPISODE_KEEP_GOLD                  VARCHAR(4)     COMMENT 'SN Space needed XD until deleted 1-14 - # days',
    REC_EPISODE_QUALITY_GOLD               VARCHAR(1)     COMMENT 'H = HD, S = SD',
    REC_EPISODE_STOP_GOLD                  VARCHAR(8)     COMMENT 'minutes before (neg) or after (pos) the scheduled endtime the user wants the recording to end',
    REC_SERIES_KEEP_GOLD                   VARCHAR(8)     COMMENT 'SN Space needed XD until deleted 1-14 - # days',
    REC_SERIES_QUALITY_GOLD                VARCHAR(4)     COMMENT 'H = HD, S = SD',
    REC_SERIES_STOP_GOLD                   VARCHAR(8)     COMMENT 'minutes before (neg) or after (pos) the scheduled endtime the user wants the recording to end',
    REC_SERIES_SAVE_NUM_EP_GOLD            VARCHAR(6)     COMMENT 'the number of episodes of a series to save (0 - all)',
    REC_SERIES_TYPE_GOLD                   VARCHAR(2)     COMMENT 'N - new only, T - any Timeslot, A - Any Time',
    RECORDER_ID_GOLD                       INT            COMMENT '',
    CREATED_BY_GOLD                        VARCHAR(64)    COMMENT '',
    MODIFY_TIME_GOLD                       TIMESTAMP      COMMENT '',
    MODIFIED_BY_GOLD                       VARCHAR(64)    COMMENT '',
    SYST_GOLD                              VARCHAR(6)     COMMENT '',
	LAST_UPDATED_GOLD                      TIMESTAMP      COMMENT '',
	OP_TYPE_GOLD                           VARCHAR(1)     COMMENT '',
	SORT_OPTION_IND                        VARCHAR(1)     COMMENT '',
	SPACE_ALLOTED_IND                      VARCHAR(1)     COMMENT '',
	SPACE_USED_IND                         VARCHAR(1)     COMMENT '',
	AUTO_ERASE_IND                         VARCHAR(1)     COMMENT '',
	SAVE_DAYS_IND                          VARCHAR(1)     COMMENT '',
	NODE_ID_IND                            VARCHAR(1)     COMMENT '',
	BLOCK_TITLES_IND                       VARCHAR(1)     COMMENT '',
	STATUS_IND                             VARCHAR(1)     COMMENT '',
	CREATE_TIME_IND                        VARCHAR(1)     COMMENT '',
	STATUS_CHANGED_IND                     VARCHAR(1)     COMMENT '',
	INGESTS_ALLOTED_IND                    VARCHAR(1)     COMMENT '',
	VERSION1_IND                           VARCHAR(1)     COMMENT '',
    REC_PAST_FOLDERS_IND                   VARCHAR(1)     COMMENT '',
    REC_EPISODE_KEEP_IND                   VARCHAR(1)     COMMENT '',
    REC_EPISODE_QUALITY_IND                VARCHAR(1)     COMMENT '',
    REC_EPISODE_STOP_IND                   VARCHAR(1)     COMMENT '',
    REC_SERIES_KEEP_IND                    VARCHAR(1)     COMMENT '',
    REC_SERIES_QUALITY_IND                 VARCHAR(1)     COMMENT '',
    REC_SERIES_STOP_IND                    VARCHAR(1)     COMMENT '',
    REC_SERIES_SAVE_NUM_EP_IND             VARCHAR(1)     COMMENT '',
    REC_SERIES_TYPE_IND                    VARCHAR(1)     COMMENT '',
    RECORDER_ID_IND                        VARCHAR(1)     COMMENT '',
    CREATED_BY_IND                         VARCHAR(1)     COMMENT '',
    MODIFY_TIME_IND                        VARCHAR(1)     COMMENT '',
    MODIFIED_BY_IND                        VARCHAR(1)     COMMENT '',
    LAST_UPDATED_IND                       VARCHAR(1)     COMMENT '',
	NEWFLAG                                VARCHAR(10)    COMMENT '',
	CHANGEDFLAG                            VARCHAR(10)    COMMENT '',
	LOAD_DATE                              TIMESTAMP      COMMENT '',
	DTM_SYSDATE                            TIMESTAMP      COMMENT '',
	Y_IND                                  VARCHAR(1)     COMMENT '',
	OP_TYPE_I                              VARCHAR(1)     COMMENT '',
	OP_TYPE_U                              VARCHAR(1)     COMMENT ''
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################