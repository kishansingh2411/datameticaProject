--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create gold_f_vod_orders_mth_corp table at Gold layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/23/2017
--#   Log File    : .../log/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/23/2017      Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
    studio_id                   INT,
    title_id                    INT,
    subscription_name_id        INT,
    subscription_type_id        INT,
    hd_flag                     STRING,
    genre_id                    INT,
    corp_id                     INT,
    preview_ind                 STRING,
    trailer_ind                 STRING,
    ecohort_code_id             INT,
    ethnic_code_id              INT,
    vod_play_time_seconds       DOUBLE,
    vod_fast_forward_cnt        DOUBLE,
    vod_rewind_cnt              DOUBLE,
    vod_pause_cnt               DOUBLE,
    vod_order_amt               BIGINT,
    vod_unique_customers_cnt    BIGINT,
    vod_unique_boxes_cnt        BIGINT,
    vod_orders_cnt              BIGINT,
    kom_last_modified_date      TIMESTAMP
)
partitioned by (month_id int)
STORED AS ORC
LOCATION '${hivevar:location}'
;
