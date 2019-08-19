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

	CALLSIGN_INCOMING              VARCHAR(12)     COMMENT '',
	STARTDATE_INCOMING             TIMESTAMP       COMMENT '',
	STARTTIME_INCOMING             VARCHAR(8)          COMMENT '',
	DURATION_INCOMING              INT          COMMENT '',
	SHORT_TITLE_INCOMING           VARCHAR(60)     COMMENT '',
	LONG_TITLE_INCOMING            VARCHAR(250)    COMMENT '',
	THEME_IDS_INCOMING             VARCHAR(25)     COMMENT '',
	SHORT_DESC_INCOMING            VARCHAR(120)    COMMENT '',
	LONG_DESC_INCOMING             VARCHAR(255)    COMMENT '',
	MPAA_RATING_INCOMING           VARCHAR(25)     COMMENT '',
	TV_RATING_INCOMING             VARCHAR(25)     COMMENT '',
	STEREO_INCOMING                CHAR(1)      COMMENT '',
	SURROUND_INCOMING              CHAR(1)      COMMENT '',
	SAP_INCOMING                   CHAR(1)      COMMENT '',
	CLOSEDCAPTION_INCOMING         CHAR(1)      COMMENT '',
	ANIMATED_INCOMING              CHAR(1)      COMMENT '',
	BLACKWHITE_INCOMING            CHAR(1)      COMMENT '',
	RERUN_INCOMING                 CHAR(1)      COMMENT '',
	LIVE_INCOMING                  CHAR(1)      COMMENT '',
	ISMOVIE_INCOMING               CHAR(1)      COMMENT '',
	NUDITY_INCOMING                CHAR(1)      COMMENT '',
	ADULTLANG_INCOMING             CHAR(1)      COMMENT '',
	VIOLENCE_INCOMING              CHAR(1)      COMMENT '',
	ADULTTHEME_INCOMING            CHAR(1)      COMMENT '',
	HALFSTARS_INCOMING             INT          COMMENT '',
	HDTV_INCOMING                  VARCHAR(1)      COMMENT '',
	CVC_CALLSIGN_INCOMING          VARCHAR(12)     COMMENT '',
	LOADTIME              TIMESTAMP       COMMENT '',
	STARTDATE_STARTTIME_INCOMING   TIMESTAMP      COMMENT '',
    DS_STARTTIME_INCOMING          TIMESTAMP      COMMENT 'start time in Eastern Time',
    LAST_UPDATED_INCOMING          TIMESTAMP COMMENT '',
	CALLSIGN_GOLD                  VARCHAR(12)     COMMENT '',
	STARTDATE_GOLD                 TIMESTAMP       COMMENT '',
	STARTTIME_GOLD                 VARCHAR(8)      COMMENT '',
	DURATION_GOLD                  INT             COMMENT '',
	SHORTTITLE_GOLD                VARCHAR(60)     COMMENT '',
	LONGTITLE_GOLD                 VARCHAR(250)    COMMENT '',
	THEMEIDS_GOLD                  VARCHAR(25)     COMMENT '',
	SHORTDESC_GOLD                 VARCHAR(120)    COMMENT '',
	LONGDESC_GOLD                  VARCHAR(255)    COMMENT '',
	MPAARATING_GOLD                VARCHAR(25)     COMMENT '',
	TVRATING_GOLD                  VARCHAR(25)     COMMENT '',
	STEREO_GOLD                    VARCHAR(1)      COMMENT '',
	SURROUND_GOLD                  VARCHAR(1)      COMMENT '',
	SAP_GOLD                       VARCHAR(1)      COMMENT '',
	CLOSEDCAPTION_GOLD             VARCHAR(1)      COMMENT '',
	ANIMATED_GOLD                  VARCHAR(1)      COMMENT '',
	BLACKWHITE_GOLD                VARCHAR(1)      COMMENT '',
	RERUN_GOLD                     VARCHAR(1)      COMMENT '',
	LIVE_GOLD                      VARCHAR(1)      COMMENT '',
	ISMOVIE_GOLD                   VARCHAR(1)      COMMENT '',
	NUDITY_GOLD                    VARCHAR(1)      COMMENT '',
	ADULTLANG_GOLD                 VARCHAR(1)      COMMENT '',
	VIOLENCE_GOLD                  VARCHAR(1)      COMMENT '',
	ADULTTHEME_GOLD                VARCHAR(1)      COMMENT '',
	HALFSTARS_GOLD                 INT             COMMENT '',
	HDTV_GOLD                      VARCHAR(1)      COMMENT '',
	CVCCALLSIGN_GOLD               VARCHAR(12)     COMMENT '',
	STARTDATE_STARTTIME_GOLD       TIMESTAMP       COMMENT '',
    DS_STARTTIME_GOLD              TIMESTAMP       COMMENT 'start time in Eastern Time',
    LAST_UPDATED_GOLD              TIMESTAMP       COMMENT '',
	OP_TYPE                        VARCHAR(1)      COMMENT '',
	SHORTTITLE_IND                 CHAR(1)      COMMENT '',
	LONGTITLE_IND                  CHAR(1)      COMMENT '',
	THEMEIDS_IND                   CHAR(1)      COMMENT '',
	SHORTDESC_IND                  CHAR(1)      COMMENT '',
	LONGDESC_IND                   CHAR(1)      COMMENT '',
	MPAARATING_IND                 CHAR(1)      COMMENT '',
	TVRATING_IND                   CHAR(1)      COMMENT '',
	STEREO_IND                     CHAR(1)      COMMENT '',
	SURROUND_IND                   CHAR(1)      COMMENT '',
	SAP_IND                        CHAR(1)      COMMENT '',
	CLOSEDCAPTION_IND              CHAR(1)      COMMENT '',
	ANIMATED_IND                   CHAR(1)      COMMENT '',
	BLACKWHITE_IND                 CHAR(1)      COMMENT '',
	RERUN_IND                      CHAR(1)      COMMENT '',
	LIVE_IND                       CHAR(1)      COMMENT '',
	ISMOVIE_IND                    CHAR(1)      COMMENT '',
	NUDITY_IND                     CHAR(1)      COMMENT '',
	ADULTLANG_IND                  CHAR(1)      COMMENT '',
	VIOLENCE_IND                   CHAR(1)      COMMENT '',
	ADULTTHEME_IND                 CHAR(1)      COMMENT '',
	HALFSTARS_IND                  CHAR(1)      COMMENT '',
	HDTV_IND                       CHAR(1)      COMMENT '',
	CVCCALLSIGN_IND                CHAR(1)      COMMENT '',
	STARTDATE_STARTTIME_IND        CHAR(1)      COMMENT '',
	DS_STARTTIME_IND               CHAR(1)      COMMENT '',
	LAST_UPDATED_IND               CHAR(1)      COMMENT '',
	CHANGEDFLAG                    VARCHAR(5)      COMMENT '',
	NEWFLAG                        VARCHAR(5)      COMMENT '',
	LOAD_DATE                      TIMESTAMP          COMMENT '',
	DTM_SYSDATE                    TIMESTAMP       COMMENT '',
	Y_IND                          VARCHAR(1)      COMMENT '',
	OP_TYPE_I                      VARCHAR(1)      COMMENT '',
	OP_TYPE_U                      VARCHAR(1)      COMMENT ''
)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################