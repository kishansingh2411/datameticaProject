--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the outgoing_fwm_device_data_tbl table from gold tables.
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
      SET_TOP_BOX_ID
     ,HOUSEHOLD_ID
     ,ZIP
     ,EMPLOYEE_STATUS
     ,ACCOUNT_STATUS
     ,SET_TOP_BOX_MODEL
     ,ZONE_NAME
     ,NODE_ID
     ,PREMIUM_CHANNELS
     ,SPORTS_PACKAGES
     ,LANGUAGE_CHANNELS
     ,DVR
     ,HDTV
     ,CABLE_MODEM_IP_ADDRESS
     ,ZIP_PLUS_4
     ,FRANCHISE_NAME
     ,HEADEND_ID
     ,HUB_ID
     ,TIER
     ,SEGMENTATION_CODE
     ,REGION
     ,PRISM_CODE
     ,SYSTEM_NAME
     ,TIVO
     ,TIVO_TSN
     ,HSD
     ,TELEPHONE
     ,SECURITY
FROM 
	${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_fwm_house_eqmt_data_table} 
WHERE 1 = 1
    AND p_yyyymmdd = '${hivevar:source_date}'
ORDER BY 
	SET_TOP_BOX_ID
;

--##############################################################################
--#                                    End                                     #
--##############################################################################