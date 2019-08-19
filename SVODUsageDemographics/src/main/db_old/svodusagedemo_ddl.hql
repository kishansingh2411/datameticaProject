------------------------------------------------------------------------------------------------------------------------
--
--  Program name: create_table.hql
--  Program type: Hive script
--  Author      : Kriti Singh
--  Date        : 05/11/2016
--
--  Description : This hive script creates the fact and dimension tables and the mapping
--  table provided by Clinton Brown
--
--  NOTE: This is a one time script to be used when creating the tables.
--
--
--  These fact, dimension and staging tables are created -
--  PROCESSED.F_VOD_ORDERS_MTH_CORP
--  PROCESSED.D_VOD_STUDIO
--  PROCESSED.D_VOD_TITLE
--  PROCESSED.D_VOD_GENRE
--  PROCESSED.D_VOD_SUBSCRIPTION_NAME
--  PROCESSED.D_VOD_SUBSCRIPTION_TYPE
--  INCOMING.VOD_SUBSCRIPTION_NAME_TYPE_MAPPING
--  INCOMING.KOM_VOD_ORDER
--  INCOMING.KOM_CUSTOMER_ACCOUNT
--  STAGING.ENCRYPTED_D_ECOHORT_CHC
--  STAGING.ENCRYPTED_D_ETHNIC_CHC
--  STAGING.STG_D_GEOG_CORP
--  STAGING.STG1_VOD_ORDERS_MTH_CORP
--  STAGING.STG2_VOD_ORDERS.MTH_CORP
--
--
--  UAT cluster namenode - hdfs://cvhdpuat
--  DEV cluster namenode - hdfs://cvldhdpds1
--  PROD cluster namenode - hdfs://cvlphdpd1
--
--
------------------------------------------------------------------------------------------------------------------------

DROP TABLE processed.d_vod_studio;

CREATE TABLE processed.d_vod_studio
(
    studio_id       INT,
    studio_desc     STRING,
    begin_date      TIMESTAMP,
    end_date        TIMESTAMP
)
STORED AS ORC LOCATION '/prcssd/svodusagedemo/d_vod_studio';

--STORED AS AVRO LOCATION '/prcssd/svodusagedemo/d_vod_studio'
--TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------


DROP TABLE processed.d_vod_title;

CREATE TABLE processed.d_vod_title
(
    title_id        INT,
    title_desc      STRING,
    begin_date      TIMESTAMP,
    end_date        TIMESTAMP
)
STORED AS ORC LOCATION '/prcssd/svodusagedemo/d_vod_title';

--STORED AS AVRO LOCATION '/prcssd/svodusagedemo/d_vod_title'
--TBLPROPERTIES ( "avro.output.codec=snappy");

------------------------------------------------------------------------------------------------------------------------


DROP TABLE processed.d_vod_subscription_name;

CREATE TABLE processed.d_vod_subscription_name
(
    subscription_name_id    INT,
    subscription_name       STRING,
    begin_date              TIMESTAMP,
    end_date                TIMESTAMP
)
STORED AS ORC LOCATION '/prcssd/svodusagedemo/d_vod_subscription_name';

--STORED AS AVRO LOCATION '/prcssd/svodusagedemo/d_vod_subscription_name'
--TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------

-- This table contains subscription types - FVOD, SVOD, VOD, TRAILER etc

--hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/text_files/subscription_type.csv /incoming/svodusagedemo/subscription_type_dir

DROP TABLE incoming.vod_subscription_type_text;

CREATE EXTERNAL TABLE incoming.vod_subscription_type_text
(
    subscription_type_id    INT,
    subscription_type       STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/incoming/svodusagedemo/subscription_type_dir';


DROP TABLE processed.d_vod_subscription_type;

CREATE TABLE processed.d_vod_subscription_type
(
    subscription_type_id    INT,
    subscription_type       STRING,
    begin_date              TIMESTAMP,
    end_date                TIMESTAMP
)
STORED AS ORC LOCATION '/prcssd/svodusagedemo/d_vod_subscription_type';

INSERT OVERWRITE TABLE processed.d_vod_subscription_type select subscription_type_id, subscription_type,
FROM_UNIXTIME(UNIX_TIMESTAMP()), FROM_UNIXTIME(UNIX_TIMESTAMP()) FROM incoming.vod_subscription_type_text;

DROP TABLE incoming.vod_subscription_type_text;

--STORED AS AVRO LOCATION '/prcssd/svodusagedemo/d_vod_subscription_type'
--TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------


DROP TABLE processed.d_vod_genre;

CREATE TABLE processed.d_vod_genre
(
    genre_id        INT,
    genre_desc      STRING,
    begin_date      TIMESTAMP,
    end_date        TIMESTAMP
)
STORED AS ORC LOCATION '/prcssd/svodusagedemo/d_vod_genre';

--STORED AS AVRO LOCATION '/prcssd/svodusagedemo/d_vod_genre'
--TBLPROPERTIES ( "avro.output.codec=snappy");



------------------------------------------------------------------------------------------------------------------------


DROP TABLE processed.f_vod_orders_mth_corp;

CREATE TABLE processed.f_vod_orders_mth_corp
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
STORED AS ORC LOCATION '/prcssd/svodusagedemo/f_vod_orders_mth_corp';

--STORED AS AVRO LOCATION '/prcssd/svodusagedemo/f_vod_orders_mth_corp'
--TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------

--This is the clinton brown table

--hdfs dfs -copyFromLocal /UTIL/app/util/SvodUsageDemographics/text_files/clinton_brown_table.csv /incoming/svodusagedemo/clinton_brown_dir

DROP TABLE incoming.vod_subscription_name_type_mapping_text;

CREATE EXTERNAL TABLE incoming.vod_subscription_name_type_mapping_text
(
    subscription_name   STRING,
    vod_type            STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '/incoming/svodusagedemo/clinton_brown_dir';


DROP TABLE incoming.vod_subscription_name_type_mapping;

CREATE TABLE incoming.vod_subscription_name_type_mapping
(
    subscription_name   STRING,
    vod_type            STRING,
    begin_date          TIMESTAMP,
    end_date            TIMESTAMP
)
STORED AS ORC LOCATION '/incoming/svodusagedemo/vod_subscription_name_type_mapping';

INSERT OVERWRITE TABLE incoming.vod_subscription_name_type_mapping SELECT subscription_name, vod_type,
FROM_UNIXTIME(UNIX_TIMESTAMP()), FROM_UNIXTIME(UNIX_TIMESTAMP()) FROM incoming.vod_subscription_name_type_mapping_text;

--STORED AS AVRO LOCATION '/incoming/svodusagedemo/vod_subscription_name_type_mapping'
--TBLPROPERTIES ( "avro.output.codec=snappy");


DROP TABLE incoming.vod_subscription_name_type_mapping_text;

------------------------------------------------------------------------------------------------------------------------


DROP TABLE incoming.kom_vod_order;

CREATE EXTERNAL TABLE incoming.kom_vod_order
(
    encrypted_customer_account_id   STRING,
    vod_order_id                    BIGINT,
    corp                            BIGINT,
    preview_ind                     STRING,
    trailer_ind                     STRING,
    dtm_created                     TIMESTAMP
)
partitioned by (month_id int)
STORED AS AVRO LOCATION '/incoming/svodusagedemo/encrypted_kom_vod_order'
TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------


DROP TABLE incoming.kom_customer_account;

CREATE EXTERNAL TABLE incoming.kom_customer_account
(
    encrypted_customer_account_id   STRING,
    encrypted_chc_id                STRING,
    dtm_last_updated                TIMESTAMP,
    dtm_created                     TIMESTAMP
)
STORED AS AVRO LOCATION '/incoming/svodusagedemo/encrypted_kom_customer_account'
TBLPROPERTIES ( "avro.output.codec=snappy");



------------------------------------------------------------------------------------------------------------------------

DROP TABLE staging.encrypted_d_ethnic_chc;

CREATE TABLE staging.encrypted_d_ethnic_chc
(
    chc_id                  BIGINT,
    encrypted_chc_id        STRING,
    ethnic_code_id          INT,
    from_month_id           INT,
    to_month_id             INT,
    kom_last_modified_date  TIMESTAMP,
    rn                      INT
)
STORED AS AVRO LOCATION '/staging/svodusagedemo/encrypted_d_ethnic_chc'
TBLPROPERTIES ( "avro.output.codec=snappy");

------------------------------------------------------------------------------------------------------------------------

DROP TABLE staging.encrypted_d_ecohort_chc;

CREATE TABLE staging.encrypted_d_ecohort_chc
(
    chc_id                  BIGINT,
    encrypted_chc_id        STRING,
    ecohort_code_id         INT,
    from_month_id           INT,
    to_month_id             INT,
    kom_last_modified_date  TIMESTAMP,
    rn                      INT
)
STORED AS AVRO LOCATION '/staging/svodusagedemo/encrypted_d_ecohort_chc'
TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------


DROP TABLE staging.stg_d_geog_corp;

CREATE TABLE staging.stg_d_geog_corp
(
    corp_id     INT,
    corp        INT,
    end_date    INT,
    rn          INT
)
STORED AS AVRO LOCATION '/staging/svodusagedemo/stg_d_geog_corp'
TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------

DROP TABLE staging.stg1_vod_orders_mth_corp;

CREATE TABLE staging.stg1_vod_orders_mth_corp
(
    month_id            	        STRING,
    studio_desc         	        STRING,
    title_desc          	        STRING,
    subscription_name   	        STRING,
    genre_desc          	        STRING,
    corp                	        BIGINT,
    preview_ind         	        STRING,
    trailer_ind         	        STRING,
    encrypted_chc_id    	        STRING,
    play_time_seconds   	        BIGINT,
    fast_forward_cnt    	        BIGINT,
    rewind_cnt          	        BIGINT,
    pause_cnt           	        BIGINT,
    order_amt           	        BIGINT,
    encrypted_customer_account_id	STRING,
    box_id              	        BIGINT
)
STORED AS AVRO LOCATION '/staging/svodusagedemo/stg1_vod_orders_mth_corp'
TBLPROPERTIES ( "avro.output.codec=snappy");

------------------------------------------------------------------------------------------------------------------------

DROP TABLE staging.stg2_vod_orders_mth_corp;

CREATE TABLE staging.stg2_vod_orders_mth_corp
(
    month_id                    STRING,
    studio_desc                 STRING,
    title_desc                  STRING,
    subscription_name           STRING,
    genre_desc                  STRING,
    corp                        BIGINT,
    preview_ind                 STRING,
    trailer_ind                 STRING,
    encrypted_chc_id            STRING,
    vod_play_time_seconds       BIGINT,
    vod_fast_forward_cnt        BIGINT,
    vod_rewind_cnt              BIGINT,
    vod_pause_cnt               BIGINT,
    vod_order_amt               BIGINT,
    vod_unique_customers_cnt    BIGINT,
    vod_unique_boxes_cnt        BIGINT,
    vod_orders_cnt              BIGINT
)
STORED AS AVRO LOCATION '/staging/svodusagedemo/stg2_vod_orders_mth_corp'
TBLPROPERTIES ( "avro.output.codec=snappy");


------------------------------------------------------------------------------------------------------------------------


DROP TABLE staging.stg3_vod_orders_mth_corp;

CREATE TABLE staging.stg3_vod_orders_mth_corp
(
    month_id                    STRING,
    studio_desc                 STRING,
    title_desc                  STRING,
    subscription_name           STRING,
    subscription_type           STRING,
    hd_flag                     STRING,
    genre_desc                  STRING,
    corp                        BIGINT,
    preview_ind                 STRING,
    trailer_ind                 STRING,
    encrypted_chc_id            STRING,
    vod_play_time_seconds       BIGINT,
    vod_fast_forward_cnt        BIGINT,
    vod_rewind_cnt              BIGINT,
    vod_pause_cnt               BIGINT,
    vod_order_amt               BIGINT,
    vod_unique_customers_cnt    BIGINT,
    vod_unique_boxes_cnt        BIGINT,
    vod_orders_cnt              BIGINT
)
STORED AS AVRO LOCATION '/staging/svodusagedemo/stg3_vod_orders_mth_corp'
TBLPROPERTIES ( "avro.output.codec=snappy");

------------------------------------------------------------------------------------------------------------------------
--
--  FINISHED
--
------------------------------------------------------------------------------------------------------------------------
