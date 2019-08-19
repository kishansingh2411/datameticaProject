--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the incoming_stg3_vod_orders_mth_corp table from incoming.
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
--#    1.0     DataMetica Team          01/22/2017       Initial version
--#
--#
--#####################################################################################################################


-- TRUNCATE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg3_vod_orders_mth_corp_table}
set mapred.reduce.tasks=100;
set hive.execution.engine=tez;

INSERT OVERWRITE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg3_vod_orders_mth_corp_table}
SELECT
    month_id,
    studio_desc,
    title_desc,
    a.subscription_name,
    CASE
        WHEN  (a.preview_ind=1 OR a.trailer_ind='Y') THEN 'TRAILER'
        WHEN (b.vod_type='SVOD') THEN 'SVOD'
        WHEN (b.vod_type='FOD' AND a.vod_order_amt = 0) THEN'FOD'
        WHEN (b.vod_type IS NULL AND a.vod_order_amt = 0) THEN 'FOD'
        WHEN (b.vod_type='FOD' AND a.vod_order_amt != 0) THEN 'PVOD'
        WHEN (b.vod_type IS NULL AND a.vod_order_amt != 0) THEN 'PVOD'
        ELSE 'N/A'
    END AS subscription_type,
    CASE
        WHEN (a.title_desc LIKE '% HD') THEN 'Y'
        WHEN (a.title_desc NOT LIKE '% HD') THEN 'N'
    END AS hd_flag,
    genre_desc,
    corp,
    preview_ind,
    trailer_ind,
    encrypted_chc_id,
    vod_play_time_seconds,
    vod_fast_forward_cnt,
    vod_rewind_cnt,
    vod_pause_cnt,
    vod_order_amt,
    vod_unique_customers_cnt,
    vod_unique_boxes_cnt,
    vod_orders_cnt
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg2_vod_orders_mth_corp_table} AS a
LEFT JOIN 
 ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_vod_subscription_name_type_mapping_table} AS b
    ON UPPER(TRIM(a.subscription_name))=UPPER(TRIM(b.subscription_name));
    
