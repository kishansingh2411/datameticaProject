--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_fwm_house_eqmt_data_tbl table from gold tables.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/23/2017
--#   Log File    : .../log/fourthwall_media/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/fourthwall_media/${job_name}.log
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


INSERT OVERWRITE DIRECTORY '${hivevar:dir_path}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'  
SELECT 
	DISTINCT HOUSEHOLD_ID, LAST_NAME, FIRST_NAME, STREET_ADDRESS, CITY, STATE, ZIP_CODE
FROM 
	${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_fwm_house_eqmt_data_table}
WHERE 1 = 1
    AND p_yyyymmdd = '${hivevar:source_date}'	
ORDER BY 
	HOUSEHOLD_ID
;

--##############################################################################
--#                                    End                                     #
--##############################################################################