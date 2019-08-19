--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold gold_export_cmts_node_tbl table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 02/05/2017
--#   Log File    : .../log/footprints/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/footprints/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          02/05/2017       Initial version
--#
--#
--#####################################################################################################################

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_export_cmts_node_table}
  PARTITION(P_YYYYMMDD)
SELECT cm.cmts_hostname
     , cm.macaddr
     , cm.IFDESCR
     , cm.IFNAME
     , cm.ifalias
     , eq.site_id
     , eq.house_nbr
     , cast(EQ.ACCT_NBR as int)
     , eq.node_cd icoms_node
     , eq.headend icoms_headend
     , from_unixtime(unix_timestamp()) time_of_update
     ,${hivevar:source_date} as p_YYYYMMDD
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_cmts_interface_mac_table} cm 
     JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_equipment_table} eq 
     ON (eq.equipment_address = cm.macaddr and cm.P_YYYYMMDD = eq.p_YYYYMMDD)
WHERE cm.P_YYYYMMDD='${hivevar:source_date}'     

--##############################################################################
--#                                    End                                     #
--##############################################################################