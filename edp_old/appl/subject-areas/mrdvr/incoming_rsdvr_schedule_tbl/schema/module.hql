--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_rsdvr_schedule_tbl)
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
	CALLSIGN            VARCHAR(12)     COMMENT 'Call sign of the channel',
	STARTDATE           TIMESTAMP       COMMENT 'Date program is to start',
	STARTTIME           VARCHAR(8)          COMMENT 'Time program is to start',
	DURATION            BIGINT          COMMENT 'Duration of the program in minutes',
	SHORT_TITLE         VARCHAR(60)     COMMENT 'Short title of the program',
	LONG_TITLE          VARCHAR(250)    COMMENT 'Long title of the program',
	THEME_IDS           VARCHAR(25)     COMMENT 'Theme ids of the program',
	SHORT_DESC          VARCHAR(120)    COMMENT 'Short Description',
	LONG_DESC           VARCHAR(255)    COMMENT 'Long Description',
	MPAA_RATING         VARCHAR(25)     COMMENT 'MPAA Rating (Movies)',
	TV_RATING           VARCHAR(25)     COMMENT 'TV Rating',
	STEREO              CHAR(1)      COMMENT 'Is program in Stereo',
	SURROUND            CHAR(1)      COMMENT 'Is program in Surround',
	SAP                 CHAR(1)      COMMENT 'Is program in Stereo',
	CLOSEDCAPTION       CHAR(1)      COMMENT 'Is program in Stereo',
	ANIMATED            CHAR(1)      COMMENT 'Is the program animated',
	BLACKWHITE          CHAR(1)      COMMENT 'Is the program in black and white',
	RERUN               CHAR(1)      COMMENT 'Is the program a rerun',
	LIVE                CHAR(1)      COMMENT 'Is the program live',
	ISMOVIE             CHAR(1)      COMMENT 'Is this a movie',
	NUDITY              CHAR(1)      COMMENT 'Does program contain nudity',
	ADULTLANG           CHAR(1)      COMMENT 'Does program contain adult language',
	VIOLENCE            CHAR(1)      COMMENT 'Is program violent',
	ADULTTHEME          CHAR(1)      COMMENT 'Is program adult themed',
	HALFSTARS           INT          COMMENT 'Star rating of the program',
	HDTV                CHAR(1)      COMMENT 'Is program in HD',
	CVC_CALLSIGN        VARCHAR(12)     COMMENT 'Original Cablevision assigned call sign.',
	LOADTIME            TIMESTAMP       COMMENT '',	
    STARTDATE_STARTTIME TIMESTAMP      COMMENT '',
    DS_STARTTIME        TIMESTAMP      COMMENT 'start time in Eastern Time',
    LAST_UPDATED         TIMESTAMP COMMENT ''
    )
PARTITIONED BY (load_date STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################