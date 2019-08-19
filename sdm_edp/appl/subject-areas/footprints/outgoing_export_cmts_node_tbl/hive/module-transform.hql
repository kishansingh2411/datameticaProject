--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the outgoing outgoing_export_cmts_node_tbl table from gold.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 02/12/2017
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
--#    1.0     DataMetica Team          02/12/2017       Initial version
--#
--#
--#####################################################################################################################

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

INSERT OVERWRITE DIRECTORY '${hivevar:dir_path}'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '~' 
SELECT 
	cm.cmts_hostname
     , cm.macaddr
     , cm.ifdescr
     , cm.ifname
     , cm.ifalias
     , cm.site_id
     , cm.house_nbr
     , cm.acct_nbr
     , cm.icoms_node
     , cm.icoms_headend
     , cm.time_of_update
     , cm.p_YYYYMMDD
FROM 
	${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_export_cmts_node_table} cm 
WHERE 
	cm.P_YYYYMMDD='${hivevar:source_date}'     

--##############################################################################
--#                                    End                                     #
--##############################################################################