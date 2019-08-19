--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_export_node_srvgrp_tbl table from gold.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/28/2017
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
--#    1.0     DataMetica Team          01/28/2017       Initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_export_node_srvgrp_table}
PARTITION(P_YYYYMMDD)
SELECT --REGION,
       substr(b.REGION, 1, locate( '~', b.region, 1) - 1)   as REGION_PS_ID,
       substr(b.REGION, locate( '~', b.region, 1) + 1)      as REGION_DESCR,
       --HEAD_END,
       NVL(substr(b.HEAD_END, 1, locate( '~', b.HEAD_END, 1) - 1), ' ') as HEAD_END_PS_ID,
       NVL(substr(b.HEAD_END, locate( '~', b.HEAD_END, 1) + 1), ' ')    as HEAD_END_DESCR,
       b.NODE_CD,
       b.BASIC,
       b.DIGITAL,
       b.DIGITAL_EQUIPMENT,
       b.DVR,
       b.EQUIPMENT,
       b.EXPANDED,
       b.HDTV,
       b.HOME_SECURITY,
       b.HSD,
       b.HSD_COMMERCIAL,
       b.PRI,
       b.PRI_EXTERNAL,
       b.PRI_INTERNAL,
       b.TELEPHONE,
       b.VOD,
       b.WIFI_HOME,
       b.WIFI_WORK,
       b.WIRE_MAINTENANCE,
       a.TIME_STAMP,
       '${hivevar:source_date}' P_YYYYMMDD
FROM   (
        SELECT unix_timestamp() AS TIME_STAMP
       ) a,
       (
        SELECT -- since the values represent the same thing, cat them
               -- together.  it removes extra rows from the ROLLUP
               CONCAT(region_ps_id,   '~', region_descr)   AS REGION
             , CONCAT(head_end_ps_id, '~', head_end_descr) AS HEAD_END
             , node_cd
             , SUM(basic) AS basic
             , SUM(digital) AS digital
             , SUM(digital) AS digital_equipment
             , SUM(dvr) AS dvr
             , SUM(equipment) AS equipment
             , SUM(expanded) AS expanded
             , SUM(hdtv) AS hdtv
             , SUM(home_security) AS home_security
             , SUM(hsd) AS hsd
             , SUM(hsd_commercial) AS hsd_commercial
             , SUM(pri) AS pri
             , SUM(pri_external) AS pri_external
             , SUM(pri_internal) AS pri_internal
             , SUM(telephone) AS telephone
             , SUM(vod) AS vod
             , SUM(wifi_home) AS wifi_home
             , SUM(wifi_work) AS wifi_work
             , SUM(wire_maintenance) AS wire_maintenance
        FROM ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_customer_service_groups_table}
        WHERE p_yyyymmdd = '${hivevar:source_date}'
        GROUP BY CONCAT(region_ps_id,   '~', region_descr) 
               , CONCAT(head_end_ps_id, '~', head_end_descr)
               , node_cd
               WITH ROLLUP
     ) b
WHERE 1 = 1 
-- remove the final summary line
AND b.region is not null
;

--##############################################################################
--#                                    End                                     #
--##############################################################################