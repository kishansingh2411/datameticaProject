--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the incoming_stg1_vod_orders_mth_corp table from incoming.
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



-- TRUNCATE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg1_vod_orders_mth_corp_table};
set mapred.reduce.tasks=100;
set hive.execution.engine=tez;


INSERT OVERWRITE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_stg1_vod_orders_mth_corp_table}
SELECT
    FROM_UNIXTIME( UNIX_TIMESTAMP(b.period_date,'yyyy-MM-dd hh:mm:ss'),'yyyyMM') AS month_id,
    c.studio AS studio_desc,
    c.title AS title_desc,
    d.name AS subscription_name,
    e.name AS genre_desc,
    f.corp,
    f.preview_ind,
    f.trailer_ind,
    g.encrypted_chc_id,
    a.play_time_seconds,
    a.fast_forward_cnt,
    a.rewind_cnt,
    a.pause_cnt,
    a.order_amt,
    g.encrypted_customer_account_id,
    a.box_id
FROM
	${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_kom_f_vod_order_table} a
    INNER JOIN 
	${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_kom_d_period_table} b
    ON a.period_id = b.period_id
    INNER JOIN  
	${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_kom_d_product_master_table} c
    ON a.product_master_id = c.product_master_id
    INNER JOIN 
	${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_kom_d_subscription_group_table} d
    ON a.subscription_group_id = d.subscription_group_id
    INNER JOIN  
	${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_kom_d_genre_table} e
    ON a.genre_id = e.genre_id
    INNER JOIN 
	${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_kom_vod_order_table} f
    ON a.vod_order_id=f.vod_order_id
    INNER JOIN  
	${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_kom_customer_account_table} g
    ON f.encrypted_customer_account_id=g.encrypted_customer_account_id
WHERE
    a.year_id=${hivevar:year} AND a.month_id=${month_id} AND (f.month_id=${hivevar:year_month_id} or f.month_id=${hivevar:year_month_id}-1);
