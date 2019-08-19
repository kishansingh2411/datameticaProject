--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Incoming table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/bering_media/BERING_MEDIA_MOVE_TO_GOLD_JOB.log
--#   SQL File    : 
--#   Error File  : .../log/bering_media/BERING_MEDIA_MOVE_TO_GOLD_JOB.log
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

INSERT OVERWRITE TABLE ${hivevar:hive_database_name_gold}.${hivevar:gold_t_f_bm_dhcp_ip_tbl} 
   PARTITION (source_date='${hivevar:source_date}')
SELECT CVAID,
   HOUSEHOLD_KEY,
   CM_MAC_ADDR,
   IP_ADDR, 
   CPE_MAC_ADDR, 
   LEASE_TIME,
   TAG,
   PROV_ACCT_FLG, 
   OOL_RESI_FLG, 
   DTM_CREATED, 
   FILE_NAME
FROM ${hivevar:hive_database_name_incoming}.${hivevar:incoming_ext_t_f_bm_dhcp_ip_tbl} 
WHERE 
   SOURCE_DATE='${hivevar:source_date}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################