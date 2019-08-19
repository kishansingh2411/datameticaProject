--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Work Tmp table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 09/06/2016
--#   Log File    : .../log/mrdvr/MRDVR_MOVE_TO_GOLD_JOB.log
--#   SQL File    : 
--#   Error File  : .../log/mrdvr/MRDVR_MOVE_TO_GOLD_JOB.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          09/12/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_schedule_delta}
	PARTITION (LOAD_DATE) 
SELECT
	CALLSIGN_INCOMING  as CALLSIGN,
	STARTDATE_INCOMING  as STARTDATE,        
	STARTTIME_INCOMING  as STARTTIME,
	DURATION_INCOMING as DURATION,
	SHORT_TITLE_INCOMING as SHORTTITLE,
	LONG_TITLE_INCOMING as LONGTITLE,
	THEME_IDS_INCOMING as THEMEIDS,
	SHORT_DESC_INCOMING as SHORTDESC,
	LONG_DESC_INCOMING as LONGDESC,
	MPAA_RATING_INCOMING as MPAARATING,       
	TV_RATING_INCOMING as TVRATING,
	STEREO_INCOMING as STEREO,
	SURROUND_INCOMING as SURROUND, 
	SAP_INCOMING as SAP,
	CLOSEDCAPTION_INCOMING as CLOSEDCAPTION,  
	ANIMATED_INCOMING as ANIMATED,
	BLACKWHITE_INCOMING as BLACKWHITE,
	RERUN_INCOMING as RERUN,
	LIVE_INCOMING  as LIVE,    
	ISMOVIE_INCOMING as ISMOVIE,     
	NUDITY_INCOMING as NUDITY,
	ADULTLANG_INCOMING as ADULTLANG,  
	VIOLENCE_INCOMING as VIOLENCE,
	ADULTTHEME_INCOMING as ADULTTHEME,
	HALFSTARS_INCOMING as HALFSTARS,
	HDTV_INCOMING as HDTV,
	CVC_CALLSIGN_INCOMING as CVCCALLSIGN,
	STARTDATE_STARTTIME_INCOMING as STARTDATE_STARTTIME,              
	DS_STARTTIME_INCOMING as DS_STARTTIME,                     
	LAST_UPDATED_INCOMING as LAST_UPDATED,                      
	SHORTTITLE_IND,
	LONGTITLE_IND,
	THEMEIDS_IND,
	SHORTDESC_IND,
	LONGDESC_IND,
	MPAARATING_IND,
	TVRATING_IND,
	STEREO_IND,   
	SURROUND_IND,   
	SAP_IND,  
	CLOSEDCAPTION_IND,
	ANIMATED_IND,
	BLACKWHITE_IND,
	RERUN_IND,
	LIVE_IND,     
	ISMOVIE_IND,
	NUDITY_IND,   
	ADULTLANG_IND,
	VIOLENCE_IND, 
	ADULTTHEME_IND,
	HALFSTARS_IND,
	HDTV_IND, 
	CVCCALLSIGN_IND,
	STARTDATE_STARTTIME_IND,
    DS_STARTTIME_IND,       
    LAST_UPDATED_IND,  
	case when WORK_RSDVR_SCHEDULE_DEDUP.CHANGEDFLAG='TRUE' then WORK_RSDVR_SCHEDULE_DEDUP.OP_TYPE_U else WORK_RSDVR_SCHEDULE_DEDUP.OP_TYPE_I end  as OP_TYPE,
	DTM_SYSDATE as DTM_CREATED,
	RSDVR_SCHEDULE_DELTA_SEQ,
	DATE_FORMAT(LOAD_DATE,'yyyy-MM-dd') as LOAD_DATE
FROM
	${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_schedule_dedup}
WHERE
	NEWFLAG='TRUE' OR CHANGEDFLAG='TRUE'
;


--##############################################################################
--#                                    End                                     #
--##############################################################################