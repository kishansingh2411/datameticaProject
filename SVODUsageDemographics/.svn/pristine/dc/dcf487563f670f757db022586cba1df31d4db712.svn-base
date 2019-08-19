------------------------------------------------------------------------------------------------------------------------
--
--   Program name: redshift_prod_ddl.sql
--   Program type: sql script
--   Author      : Kriti Singh
--   Date        : 05/29/2016
--
--   Description : This sql script creates the tables in the redshift production database
--
--
------------------------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS d_vod_title CASCADE;

CREATE TABLE d_vod_title
(
   title_id    integer,
   title_desc  varchar(256),
   begin_date  timestamp,
   end_date    timestamp
);

GRANT SELECT ON d_vod_title TO bisusr;
GRANT SELECT ON d_vod_title TO rsmsusr;
GRANT SELECT ON d_vod_title TO group bis_select_dev;
GRANT SELECT ON d_vod_title TO rstabusr;


------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS d_vod_genre CASCADE;

CREATE TABLE d_vod_genre
(
   genre_id    integer,
   genre_desc  varchar(256),
   begin_date  timestamp,
   end_date    timestamp
);

GRANT SELECT ON d_vod_genre TO bisusr;
GRANT SELECT ON d_vod_genre TO rsmsusr;
GRANT SELECT ON d_vod_genre TO group bis_select_dev;
GRANT SELECT ON d_vod_genre TO rstabusr;


------------------------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS d_vod_studio CASCADE;

CREATE TABLE d_vod_studio
(
   studio_id    integer,
   studio_desc  varchar(256),
   begin_date   timestamp,
   end_date     timestamp
);

GRANT SELECT ON d_vod_studio TO bisusr;
GRANT SELECT ON d_vod_studio TO rsmsusr;
GRANT SELECT ON d_vod_studio TO group bis_select_dev;
GRANT SELECT ON d_vod_studio TO rstabusr;


------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS d_vod_subscription_name CASCADE;

CREATE TABLE d_vod_subscription_name
(
   subscription_name_id  integer,
   subscription_name     varchar(256),
   begin_date            timestamp,
   end_date              timestamp
);

GRANT SELECT ON d_vod_subscription_name TO bisusr;
GRANT SELECT ON d_vod_subscription_name TO rsmsusr;
GRANT SELECT ON d_vod_subscription_name TO group bis_select_dev;
GRANT SELECT ON d_vod_subscription_name TO rstabusr;


------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS d_vod_subscription_type CASCADE;

CREATE TABLE d_vod_subscription_type
(
   subscription_type_id  integer,
   subscription_type     varchar(256),
   begin_date            timestamp,
   end_date              timestamp
);

GRANT SELECT ON d_vod_subscription_type TO bisusr;
GRANT SELECT ON d_vod_subscription_type TO rsmsusr;
GRANT SELECT ON d_vod_subscription_type TO group bis_select_dev;
GRANT SELECT ON d_vod_subscription_type TO rstabusr;


------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS f_vod_orders_mth_corp CASCADE;

CREATE TABLE f_vod_orders_mth_corp
(
   month_id                  integer,
   studio_id                 integer,
   title_id                  integer,
   subscription_name_id      integer,
   subscription_type_id      integer,
   hd_flag                   varchar(256),
   genre_id                  integer,
   corp_id                   integer,
   preview_ind               varchar(256),
   trailer_ind               varchar(256),
   ecohort_code_id           integer,
   ethnic_code_id            integer,
   vod_play_time_seconds     numeric(18),
   vod_fast_forward_cnt      numeric(18),
   vod_rewind_cnt            numeric(18),
   vod_pause_cnt             numeric(18),
   vod_order_amt             bigint,
   vod_unique_customers_cnt  bigint,
   vod_unique_boxes_cnt      bigint,
   vod_orders_cnt            bigint,
   kom_last_modified_date    timestamp
);

GRANT SELECT ON f_vod_orders_mth_corp TO bisusr;
GRANT SELECT ON f_vod_orders_mth_corp TO rsmsusr;
GRANT SELECT ON f_vod_orders_mth_corp TO group bis_select_dev;
GRANT SELECT ON f_vod_orders_mth_corp TO rstabusr;

------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS vod_subscription_name_type_mapping CASCADE;

CREATE TABLE vod_subscription_name_type_mapping
(
    subscription_name   varchar(256),
    vod_type            varchar(256),
    begin_date          timestamp,
    end_date            timestamp
);

GRANT SELECT ON vod_subscription_name_type_mapping TO bisusr;
GRANT SELECT ON vod_subscription_name_type_mapping TO rsmsusr;
GRANT SELECT ON vod_subscription_name_type_mapping TO group bis_select_dev;
GRANT SELECT ON vod_subscription_name_type_mapping TO rstabusr;

------------------------------------------------------------------------------------------------------------------------
