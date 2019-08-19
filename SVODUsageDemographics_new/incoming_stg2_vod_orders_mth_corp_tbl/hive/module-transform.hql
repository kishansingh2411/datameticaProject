--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the incoming_stg2_vod_orders_mth_corp table from incoming.
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



-- TRUNCATE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg2_vod_orders_mth_corp_table};
set mapred.reduce.tasks=100;
set hive.execution.engine=tez;


INSERT OVERWRITE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg2_vod_orders_mth_corp_table}
SELECT
    month_id,
    studio_desc,
    title_desc,
    subscription_name,
    genre_desc,
    corp,
    preview_ind,
    trailer_ind,
    encrypted_chc_id,
    SUM(play_time_seconds) AS vod_play_time_seconds,
    SUM(fast_forward_cnt) AS vod_fast_forward_cnt,
    SUM(rewind_cnt) AS vod_rewind_cnt,
    SUM(pause_cnt) AS vod_pause_cnt,
    SUM(order_amt) AS vod_order_amt,
    COUNT(DISTINCT encrypted_customer_account_id) AS vod_unique_customers_cnt,
    COUNT(DISTINCT box_id) AS vod_unique_boxes_cnt,
    SUM(1) AS vod_orders_cnt
FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg1_vod_orders_mth_corp_table}
GROUP BY
    month_id,
    studio_desc,
    title_desc,
    subscription_name,
    genre_desc,
    corp,
    preview_ind,
    trailer_ind,
    encrypted_chc_id;
    