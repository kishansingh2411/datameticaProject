--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_f_vod_orders_mth_corp_tbl table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/22/2017
--#   Log File    : .../log/sdm/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/sdm/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#    1.0     DataMetica Team          03/22/2017       Initial version
--#
--#
--#####################################################################################################################

-- ALTER TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_f_vod_orders_mth_corp_table}
-- DROP IF EXISTS PARTITION(month_id='${hivevar:year_month_id}');
set mapred.reduce.tasks=150;
set hive.execution.engine=tez; 
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE  ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_f_vod_orders_mth_corp_table}
partition(month_id)
SELECT
    COALESCE( a.studio_id, -1) AS studio_id,
    COALESCE( b.title_id, -1) AS title_id,
    COALESCE( c.subscription_name_id, -1) AS subscription_name_id,
    COALESCE( d.subscription_type_id, -1) AS subscription_type_id,
    stg.hd_flag,
    COALESCE( e.genre_id, -1) AS genre_id,
    f.corp_id,
    stg.preview_ind,
    stg.trailer_ind,
    g.ecohort_code_id,
    h.ethnic_code_id,
    stg.vod_play_time_seconds,
    stg.vod_fast_forward_cnt,
    stg.vod_rewind_cnt,
    stg.vod_pause_cnt,
    stg.vod_order_amt,
    stg.vod_unique_customers_cnt,
    stg.vod_unique_boxes_cnt,
    stg.vod_orders_cnt,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    stg.month_id as month_id
FROM  
 ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg3_vod_orders_mth_corp_table} AS stg
INNER JOIN 
  ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_encrypted_d_ecohort_chc_table} AS g
    ON (stg.encrypted_chc_id=g.encrypted_chc_id)
INNER JOIN 
${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_encrypted_d_ethnic_chc_table} AS h
    ON (stg.encrypted_chc_id=h.encrypted_chc_id)
INNER JOIN  
${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg_d_geog_corp_table} AS f
    ON (stg.corp=f.corp)
LEFT JOIN 
${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_d_vod_studio_table} AS a
    ON (UPPER(stg.studio_desc)=UPPER(a.studio_desc))
LEFT JOIN 
${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_d_vod_title_table} AS b
    ON (UPPER(stg.title_desc)=UPPER(b.title_desc))
LEFT JOIN  
 ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_d_vod_subscription_name_table} AS c
    ON (UPPER(TRIM(stg.subscription_name))=UPPER(TRIM(c.subscription_name)))
LEFT JOIN  
${hivevar:cam_gold_database}.${hivevar:table_prefix}${hivevar:gold_d_vod_subscription_type_table} AS d
    ON (UPPER(TRIM(stg.subscription_type))=UPPER(TRIM(d.subscription_type)))
LEFT JOIN 
${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_d_vod_genre_table} AS e
    ON (UPPER(stg.genre_desc)=UPPER(e.genre_desc));
