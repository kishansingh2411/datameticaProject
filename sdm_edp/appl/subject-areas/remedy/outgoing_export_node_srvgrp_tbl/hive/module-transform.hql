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
--#   Date        : 01/30/2017
--#   Log File    : .../log/remedy/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/remedy/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          01/30/2017       Initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE DIRECTORY '${hivevar:dir_path}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '~'  
SELECT 
  REGION_PS_ID  ,
  REGION_DESCR  ,
  HEAD_END_PS_ID  ,
  HEAD_END_DESCR  ,
  NODE_CD   ,
  BASIC    ,
  DIGITAL    ,
  DIGITAL_EQUIPMENT    ,
  DVR    ,
  EQUIPMENT    ,
  EXPANDED    ,
  HDTV    ,
  HOME_SECURITY    ,
  HSD    ,
  HSD_COMMERCIAL    ,
  PRI    ,
  PRI_EXTERNAL    ,
  PRI_INTERNAL    ,
  TELEPHONE    ,
  VOD    ,
  WIFI_HOME    ,
  WIFI_WORK    ,
  WIRE_MAINTENANCE    ,
  TIME_STAMP    
FROM 
	${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_export_node_srvgrp_table}
WHERE 
	P_YYYYMMDD='${hivevar:source_date}'     	
;

--##############################################################################
--#                                    End                                     #
--##############################################################################