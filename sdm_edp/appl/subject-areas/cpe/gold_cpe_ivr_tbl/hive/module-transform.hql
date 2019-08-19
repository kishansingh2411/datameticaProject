--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_cpe_ivr table from gold tables.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/23/2017
--#   Log File    : .../log/cpe/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/cpe/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          01/23/2017       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_cpe_ivr_table} PARTITION(P_YYYYMMDD)
SELECT 
	 siteid   site_id
     , acctnum  acct_nbr
     , SUM(CASE WHEN DEPT IN (4,5,6) THEN 1 ELSE 0 END)                          IVR_TECH_TOTAL
     , SUM(CASE WHEN DEPT IN (4,5,6) AND TRANSFER_ATTEMPT <> 'Y' THEN 1 ELSE 0 END) IVR_TECH_CONTAIN
     , SUM(CASE WHEN DEPT IN (4,5,6) AND TRANSFER_ATTEMPT  = 'Y' THEN 1 ELSE 0 END) IVR_TECH_OFFER
     -- 
     , SUM(CASE WHEN DEPT = 4 THEN 1 ELSE 0 END)                          IVR_TECH_VIDEO_TOTAL
     , SUM(CASE WHEN DEPT = 4 AND TRANSFER_ATTEMPT <> 'Y' THEN 1 ELSE 0 END) IVR_TECH_VIDEO_CONTAIN
     , SUM(CASE WHEN DEPT = 4 AND TRANSFER_ATTEMPT  = 'Y' THEN 1 ELSE 0 END) IVR_TECH_VIDEO_OFFER
     -- 
     , SUM(CASE WHEN DEPT = 5 THEN 1 ELSE 0 END)                          IVR_TECH_DATA_TOTAL
     , SUM(CASE WHEN DEPT = 5 AND TRANSFER_ATTEMPT <> 'Y' THEN 1 ELSE 0 END) IVR_TECH_DATA_CONTAIN
     , SUM(CASE WHEN DEPT = 5 AND TRANSFER_ATTEMPT  = 'Y' THEN 1 ELSE 0 END) IVR_TECH_DATA_OFFER
     -- 
     , SUM(CASE WHEN DEPT = 6 THEN 1 ELSE 0 END)                          IVR_TECH_PHONE_TOTAL
     , SUM(CASE WHEN DEPT = 6 AND TRANSFER_ATTEMPT <> 'Y' THEN 1 ELSE 0 END) IVR_TECH_PHONE_CONTAIN
     , SUM(CASE WHEN DEPT = 6 AND TRANSFER_ATTEMPT  = 'Y' THEN 1 ELSE 0 END) IVR_TECH_PHONE_OFFER
             -- 
     , '${hivevar:source_date}' AS P_YYYYMMDD
FROM 
	${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_cdr_resi_table}
WHERE 
	1 = 1
    AND   NVL(siteid, 0) > 0
    AND   NVL(acctnum, 0)> 0
    AND P_YYYYMMDD='${hivevar:source_date}'
    --AND   P_YYYY = SUBSTR('${hivevar:source_date}', 1, 4) 
    --AND   P_MM   = SUBSTR('${hivevar:source_date}', 5, 2) 
    --AND   P_DD   = SUBSTR('${hivevar:source_date}', 7, 2) 
GROUP BY 
	siteid, acctnum
DISTRIBUTE BY P_YYYYMMDD
;

--##############################################################################
--#                                    End                                     #
--##############################################################################