--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build work_rsdvr_requests_archive_tmp table 
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/mrdvr/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/mrdvr/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_requests_archive_delete_dedup_tbl} 
	
SELECT 
	SCHEDULE_ID,
	HOME_ID,
	SERIAL_NUMBER,
	MODIFY_TIME,
	OP_TYPE,
	DTM_CREATED,
	RSDVR_REQUESTS_ARCHIVE_DELTA_SEQ
FROM 
(SELECT 
	delete_tbl.*,
	ROW_NUMBER () OVER (PARTITION BY SCHEDULE_ID,HOME_ID,SERIAL_NUMBER ORDER BY MODIFY_TIME DESC ) AS RECORD_ROW_NUM
FROM 
	${hivevar:hive_database_name_work}.${hivevar:work_rsdvr_requests_archive_delete_tbl} delete_tbl
WHERE 
	OP_TYPE = 'D'
)dedup
WHERE 
	dedup.RECORD_ROW_NUM = 1
;

--##############################################################################
--#                                    End                                     #
--##############################################################################