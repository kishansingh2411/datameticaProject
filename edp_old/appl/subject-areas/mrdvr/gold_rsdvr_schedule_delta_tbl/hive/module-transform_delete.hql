--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform_delete.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build gold_schedule table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform_delete.hql
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

INSERT INTO TABLE ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_schedule_delta} 
	PARTITION (LOAD_DATE)
SELECT 
GOLD_RSDVR_SCHEDULE_LAST_REC.CALLSIGN                
,GOLD_RSDVR_SCHEDULE_LAST_REC.STARTDATE               
,GOLD_RSDVR_SCHEDULE_LAST_REC.STARTTIME               
,GOLD_RSDVR_SCHEDULE_LAST_REC.DURATION                
,GOLD_RSDVR_SCHEDULE_LAST_REC.SHORTTITLE              
,GOLD_RSDVR_SCHEDULE_LAST_REC.LONGTITLE               
,GOLD_RSDVR_SCHEDULE_LAST_REC.THEMEIDS                
,GOLD_RSDVR_SCHEDULE_LAST_REC.SHORTDESC               
,GOLD_RSDVR_SCHEDULE_LAST_REC.LONGDESC                
,GOLD_RSDVR_SCHEDULE_LAST_REC.MPAARATING              
,GOLD_RSDVR_SCHEDULE_LAST_REC.TVRATING                
,GOLD_RSDVR_SCHEDULE_LAST_REC.STEREO                  
,GOLD_RSDVR_SCHEDULE_LAST_REC.SURROUND                
,GOLD_RSDVR_SCHEDULE_LAST_REC.SAP                     
,GOLD_RSDVR_SCHEDULE_LAST_REC.CLOSEDCAPTION           
,GOLD_RSDVR_SCHEDULE_LAST_REC.ANIMATED                
,GOLD_RSDVR_SCHEDULE_LAST_REC.BLACKWHITE              
,GOLD_RSDVR_SCHEDULE_LAST_REC.RERUN                   
,GOLD_RSDVR_SCHEDULE_LAST_REC.LIVE                    
,GOLD_RSDVR_SCHEDULE_LAST_REC.ISMOVIE                 
,GOLD_RSDVR_SCHEDULE_LAST_REC.NUDITY                  
,GOLD_RSDVR_SCHEDULE_LAST_REC.ADULTLANG               
,GOLD_RSDVR_SCHEDULE_LAST_REC.VIOLENCE                
,GOLD_RSDVR_SCHEDULE_LAST_REC.ADULTTHEME              
,GOLD_RSDVR_SCHEDULE_LAST_REC.HALFSTARS               
,GOLD_RSDVR_SCHEDULE_LAST_REC.HDTV                    
,GOLD_RSDVR_SCHEDULE_LAST_REC.CVCCALLSIGN             
,GOLD_RSDVR_SCHEDULE_LAST_REC.STARTDATE_STARTTIME     
,GOLD_RSDVR_SCHEDULE_LAST_REC.DS_STARTTIME            
,GOLD_RSDVR_SCHEDULE_LAST_REC.LAST_UPDATED            
,GOLD_RSDVR_SCHEDULE_LAST_REC.SHORTTITLE_IND          
,GOLD_RSDVR_SCHEDULE_LAST_REC.LONGTITLE_IND           
,GOLD_RSDVR_SCHEDULE_LAST_REC.THEMEIDS_IND            
,GOLD_RSDVR_SCHEDULE_LAST_REC.SHORTDESC_IND           
,GOLD_RSDVR_SCHEDULE_LAST_REC.LONGDESC_IND            
,GOLD_RSDVR_SCHEDULE_LAST_REC.MPAARATING_IND          
,GOLD_RSDVR_SCHEDULE_LAST_REC.TVRATING_IND            
,GOLD_RSDVR_SCHEDULE_LAST_REC.STEREO_IND              
,GOLD_RSDVR_SCHEDULE_LAST_REC.SURROUND_IND            
,GOLD_RSDVR_SCHEDULE_LAST_REC.SAP_IND                 
,GOLD_RSDVR_SCHEDULE_LAST_REC.CLOSEDCAPTION_IND       
,GOLD_RSDVR_SCHEDULE_LAST_REC.ANIMATED_IND            
,GOLD_RSDVR_SCHEDULE_LAST_REC.BLACKWHITE_IND          
,GOLD_RSDVR_SCHEDULE_LAST_REC.RERUN_IND               
,GOLD_RSDVR_SCHEDULE_LAST_REC.LIVE_IND                
,GOLD_RSDVR_SCHEDULE_LAST_REC.ISMOVIE_IND             
,GOLD_RSDVR_SCHEDULE_LAST_REC.NUDITY_IND              
,GOLD_RSDVR_SCHEDULE_LAST_REC.ADULTLANG_IND           
,GOLD_RSDVR_SCHEDULE_LAST_REC.VIOLENCE_IND            
,GOLD_RSDVR_SCHEDULE_LAST_REC.ADULTTHEME_IND          
,GOLD_RSDVR_SCHEDULE_LAST_REC.HALFSTARS_IND           
,GOLD_RSDVR_SCHEDULE_LAST_REC.HDTV_IND                
,GOLD_RSDVR_SCHEDULE_LAST_REC.CVCCALLSIGN_IND 
,GOLD_RSDVR_SCHEDULE_LAST_REC.STARTDATE_STARTTIME_IND
,GOLD_RSDVR_SCHEDULE_LAST_REC.DS_STARTTIME_IND
,GOLD_RSDVR_SCHEDULE_LAST_REC.LAST_UPDATED_IND        
,WORK_RSDVR_SCHEDULE_DELETE_DEDUP.OP_TYPE                 
,WORK_RSDVR_SCHEDULE_DELETE_DEDUP.DTM_CREATED             
,GOLD_RSDVR_SCHEDULE_LAST_REC.RSDVR_SCHEDULE_DELTA_SEQ
,DATE_FORMAT('${hivevar:source_date}','yyyy-MM-dd') as LOAD_DATE
FROM 
	${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_schedule_last_rec} GOLD_RSDVR_SCHEDULE_LAST_REC INNER JOIN ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_schedule_delete_dedup} WORK_RSDVR_SCHEDULE_DELETE_DEDUP 
ON
	(GOLD_RSDVR_SCHEDULE_LAST_REC.CALLSIGN = WORK_RSDVR_SCHEDULE_DELETE_DEDUP.CALLSIGN AND
	GOLD_RSDVR_SCHEDULE_LAST_REC.STARTDATE = WORK_RSDVR_SCHEDULE_DELETE_DEDUP.STARTDATE AND
	GOLD_RSDVR_SCHEDULE_LAST_REC.STARTTIME = WORK_RSDVR_SCHEDULE_DELETE_DEDUP.STARTTIME AND
	GOLD_RSDVR_SCHEDULE_LAST_REC.DURATION = WORK_RSDVR_SCHEDULE_DELETE_DEDUP.DURATION)
WHERE
	GOLD_RSDVR_SCHEDULE_LAST_REC.RSDVR_SCHEDULE_DELTA_SEQ=WORK_RSDVR_SCHEDULE_DELETE_DEDUP.RSDVR_SCHEDULE_DELTA_SEQ		
;