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

INSERT OVERWRITE TABLE ${hivevar:hive_database_name_gold}.${hivevar:gold_rsdvr_requests_archive_delta}
   PARTITION(LOAD_DATE)
SELECT
	SCHEDULE_ID_INCOMING as SCHEDULE_ID,
	HOME_ID_INCOMING as HOME_ID,
	SERIAL_NUMBER_INCOMING as SERIAL_NUMBER,
	MAC_ADDRESS_INCOMING as MAC_ADDRESS,
	CALL_SIGN_INCOMING as CALL_SIGN,
	START_DATE_INCOMING as START_DATE,
	START_TIME_INCOMING as START_TIME,
	DURATION_INCOMING as DURATION,
	RECORDING_TYPE_INCOMING as RECORDING_TYPE,
	SAVE_ALL_INCOMING as SAVE_ALL,
	SAVE_LATEST_INCOMING as SAVE_LATEST,
	CREATE_TIME_INCOMING as CREATE_TIME,
	MODIFIED_TIME_INCOMING as MODIFIED_TIME,
	END_TIME_ADJUSTMENT_INCOMING as END_TIME_ADJUSTMENT,
	SAVE_DAYS_INCOMING as SAVE_DAYS,
    STATUS_INCOMING as STATUS,
    SHORT_TITLE_INCOMING as SHORT_TITLE,
    ARCHIVE_DATE_EST_INCOMING as ARCHIVE_DATE_EST,
    SOURCE_LOAD_DATE_INCOMING as SOURCE_LOAD_DATE,
	MAC_ADDRESS_IND,
	CALL_SIGN_IND,
	START_DATE_IND,
	START_TIME_IND,
	DURATION_IND,
	RECORDING_TYPE_IND,
	SAVE_ALL_IND,
	SAVE_LATEST_IND,
	CREATE_TIME_IND,
	MODIFIED_TIME_IND,
	END_TIME_ADJUSTMENT_IND,
	SAVE_DAYS_IND,
	STATUS_IND,
	SHORT_TITLE_IND,
	ARCHIVE_DATE_EST_IND,
	SOURCE_LOAD_DATE_IND,
	case when WORK_RSDVR_REQUESTS_ARCHIVE_DEDUP.CHANGEDFLAG='TRUE' then WORK_RSDVR_REQUESTS_ARCHIVE_DEDUP.OP_TYPE_U else ( case when WORK_RSDVR_REQUESTS_ARCHIVE_DEDUP.NEWFLAG='TRUE' then WORK_RSDVR_REQUESTS_ARCHIVE_DEDUP.OP_TYPE_I end) end as OP_TYPE,
	DTM_SYSDATE as DTM_CREATED,
	RSDVR_REQUESTS_ARCHIVE_DELTA_SEQ,
	DATE_FORMAT(LOAD_DATE,'yyyy-MM-dd') as LOAD_DATE
FROM
	${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_requests_archive_dedup_tbl}
WHERE
   NEWFLAG='TRUE' OR CHANGEDFLAG='TRUE'
;


--##############################################################################
--#                                    End                                     #
--##############################################################################