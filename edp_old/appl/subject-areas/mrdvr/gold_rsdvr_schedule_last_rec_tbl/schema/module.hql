--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold table(gold_rsdvr_schedule_last_rec tbl)
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
		CALLSIGN                 VARCHAR(12)            COMMENT 'Call sign of the channel' ,
		STARTDATE                TIMESTAMP              COMMENT 'Date program is to start' ,
		STARTTIME                VARCHAR(8)             COMMENT 'Time program is to start' ,
		DURATION                 INT                    COMMENT 'Duration of the program in minutes' ,
		SHORTTITLE               VARCHAR(60)            COMMENT 'Short title of the program' ,
		LONGTITLE                VARCHAR(250)           COMMENT 'Long title of the program' ,
		THEMEIDS                 VARCHAR(25)            COMMENT 'Theme ids of the program' ,
		SHORTDESC                VARCHAR(120)           COMMENT 'Short Description' ,
		LONGDESC                 VARCHAR(255)           COMMENT 'Long Description' ,
		MPAARATING               VARCHAR(25)            COMMENT 'MPAA Rating (Movies)' ,
		TVRATING                 VARCHAR(25)            COMMENT 'TV Rating' ,
		STEREO                   CHAR(1)             COMMENT 'Is program in Stereo' ,
		SURROUND                 CHAR(1)             COMMENT 'Is program in Surround' ,
		SAP                      CHAR(1)             COMMENT 'Is program in SAP' ,
		CLOSEDCAPTION            CHAR(1)             COMMENT 'Is program in CLOSEDCAPTION' ,
		ANIMATED                 CHAR(1)             COMMENT 'Is the program animated' ,
		BLACKWHITE               CHAR(1)             COMMENT 'Is the program in black and white' ,
		RERUN                    CHAR(1)             COMMENT 'Is the program a rerun' ,
		LIVE                     CHAR(1)             COMMENT 'Is the program live' ,
		ISMOVIE                  CHAR(1)             COMMENT 'Is this a movie' ,
		NUDITY                   CHAR(1)             COMMENT 'Does program contain nudity' ,
		ADULTLANG                CHAR(1)             COMMENT 'Does program contain adult language' ,
		VIOLENCE                 CHAR(1)             COMMENT 'Is program violent' ,
		ADULTTHEME               CHAR(1)             COMMENT 'Is program adult themed' ,
		HALFSTARS                INT                 COMMENT 'Star rating of the program' ,
		HDTV                     CHAR(1)             COMMENT 'Is program in HD' ,
		CVCCALLSIGN              VARCHAR(12)         COMMENT 'Original Cablevision assigned call sign.' ,
		STARTDATE_STARTTIME      TIMESTAMP              COMMENT '',
        DS_STARTTIME             TIMESTAMP              COMMENT 'start time in Eastern Time',
        LAST_UPDATED             TIMESTAMP              COMMENT ''  ,
		SHORTTITLE_IND           CHAR(1)             COMMENT '' ,
		LONGTITLE_IND            CHAR(1)             COMMENT '' ,
		THEMEIDS_IND             CHAR(1)             COMMENT '' ,
		SHORTDESC_IND            CHAR(1)             COMMENT '' ,
		LONGDESC_IND             CHAR(1)             COMMENT '' ,
		MPAARATING_IND           CHAR(1)             COMMENT '' ,
		TVRATING_IND             CHAR(1)             COMMENT '' ,
		STEREO_IND               CHAR(1)             COMMENT '' ,
		SURROUND_IND             CHAR(1)             COMMENT '' ,
		SAP_IND                  CHAR(1)             COMMENT '' ,
		CLOSEDCAPTION_IND        CHAR(1)             COMMENT '' ,
		ANIMATED_IND             CHAR(1)             COMMENT '' ,
		BLACKWHITE_IND           CHAR(1)             COMMENT '' ,
		RERUN_IND                CHAR(1)             COMMENT '' ,
		LIVE_IND                 CHAR(1)             COMMENT '' ,
		ISMOVIE_IND              CHAR(1)             COMMENT '' ,
		NUDITY_IND               CHAR(1)             COMMENT '' ,
		ADULTLANG_IND            CHAR(1)             COMMENT '' ,
		VIOLENCE_IND             CHAR(1)             COMMENT '' ,
		ADULTTHEME_IND           CHAR(1)             COMMENT '' ,
		HALFSTARS_IND            CHAR(1)             COMMENT '' ,
		HDTV_IND                 CHAR(1)             COMMENT '' ,
		CVCCALLSIGN_IND          CHAR(1)             COMMENT '' ,
		STARTDATE_STARTTIME_IND  CHAR(1)             COMMENT '',
	    DS_STARTTIME_IND         CHAR(1)             COMMENT '',
	    LAST_UPDATED_IND         CHAR(1)             COMMENT '',
		OP_TYPE                  VARCHAR(1)             COMMENT '' ,
		DTM_CREATED              TIMESTAMP              COMMENT '' ,
		RSDVR_SCHEDULE_DELTA_SEQ BIGINT                 COMMENT '' ,
		DTM_LAST_MODIFIED        TIMESTAMP              COMMENT '' ,
		DTM_LAST_UPDATED         TIMESTAMP              COMMENT '' ,
		LOAD_DATE                TIMESTAMP              COMMENT ''
)
STORED AS ORC
LOCATION '${hivevar:location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;

--##############################################################################
--#                                    End                                     #
--##############################################################################