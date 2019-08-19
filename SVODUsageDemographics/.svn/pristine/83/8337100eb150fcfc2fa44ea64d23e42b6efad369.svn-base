------------------------------------------------------------------------------------------------------------------------
--
--   Program name: vod_usage_by_customer_attributes.hql
--   Program type: Hive script
--   Author      : Kriti Singh
--   Date        : 05/11/2016
--
--   Description : This hive script populates the fact table and dimension tables.
--   The fact table has aggregated data of vod/svod/fvod usage by customer
--   demographic attributes.
--
--   The BRD can be found at the svn location -
--   svn/appbis/preqs/vod/trunk/docs/VOD_REPORTING_DOCUMENT.docx
--
--   The list of Fact and Dimension tables are -
--   F_VOD_ORDERS_MTH_CORP
--   D_VOD_STUDIO
--   D_VOD_TITLE
--   D_VOD_GENRE
--   D_VOD_SUBSCRIPTION_NAME
--   D_VOD_SUBSCRIPTION_TYPE
--
--
--   The list of sourced tables from CVIM1P, CVCDRP, CVRBISP -
--   CVIM1P.VODUSR.KOM_F_VOD_ORDER
--   CVCDRP.CDRUSR.KOM_VOD_ORDER
--   CVCDRP.CDRUSR.KOM_CUSTOMER_ACCOUNT
--   CVIM1P.VODUSR.KOM_D_PERIOD
--   CVIM1P.VODUSR.KOM_D_PRODUCT_MASTER
--   CVIM1P.VODUSR.KOM_D_SUBSCRIPTION_GROUP
--   CVIM1P.VODUSR.KOM_D_VOD_GENRE
--   CVRBISP.BISMGR.D_ECOHORT_CHC
--   CVRBISP.BISMGR.D_ETHNIC_CHC
--   CVRBISP.BISMGR.D_GEOG_CORP
--
--
--   VOD_SUBSCRIPTION_NAME_TYPE_MAPPING - This table will be maintained by Clinton Brown.
--   It has the mapping of subscription_name to vod_type.
--
--   NOTE: Customer will provide any new subscription_type dimension. Currently supported
--   subscription_types are - TRAILER, FOD, SVOD, PVOD
--
-- TODO: change the d_ecohort_chc and d_ethnic_chc
------------------------------------------------------------------------------------------------------------------------




------------------------------------------------------------------------------------------------------------------------
--
--   Stage 1
--
--   Populate the dimension tables - d_vod_studio, d_vod_title, d_vod_genre, d_vod_subscription_name
--
--   d_vod_subscription_type dimension table currently has 5 values - FOD, SVOD, PVOD, TRAILER, N/A
--   This table is manually maintained
--
--   When a new record is found in the incoming table, it is added to the dimension
--   table with an auto-increment id
--
------------------------------------------------------------------------------------------------------------------------

--   Time taken by this query on dev cluster = 16 secs

INSERT INTO processed.d_vod_studio
(
    studio_id,
    studio_desc,
    begin_date,
    end_date
)
SELECT
    seq_gen('studio_id',1000,1L),
    a.studio,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM
(
    SELECT DISTINCT UPPER(TRIM(studio)) AS studio FROM incoming.kom_d_product_master
) AS a
LEFT JOIN processed.d_vod_studio AS b
    ON (UPPER(TRIM(a.studio)) = UPPER(TRIM(b.studio_desc)))
WHERE b.studio_id IS NULL;


------------------------------------------------------------------------------------------------------------------------


--   Time taken by this query on dev cluster = 36 secs

INSERT INTO processed.d_vod_title
(
    title_id,
    title_desc,
    begin_date,
    end_date
)
SELECT
    seq_gen('title_id',1000,1L),
    a.title,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM
(
    SELECT DISTINCT UPPER(TRIM(title)) AS title FROM incoming.kom_d_product_master
) AS a
LEFT JOIN processed.d_vod_title AS b
    ON (UPPER(TRIM(a.title)) = UPPER(TRIM(b.title_desc)))
WHERE b.title_id IS NULL;


------------------------------------------------------------------------------------------------------------------------


--   Time taken by this query on dev cluster = 6 secs

INSERT INTO processed.d_vod_genre
(
    genre_id,
    genre_desc,
    begin_date,
    end_date
)
SELECT
    seq_gen('genre_id',1000,1L),
    a.name,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM
(
    SELECT DISTINCT UPPER(TRIM(name)) name FROM incoming.kom_d_genre
) AS a
LEFT JOIN processed.d_vod_genre AS b
    ON (UPPER(TRIM(a.name)) = UPPER(TRIM(b.genre_desc)))
WHERE b.genre_id IS NULL;


------------------------------------------------------------------------------------------------------------------------


--   Time taken by this query on dev cluster = 3.8 secs

INSERT INTO processed.d_vod_subscription_name
(
    subscription_name_id,
    subscription_name,
    begin_date,
    end_date
)
SELECT
    seq_gen('subscription_name_id',1000,1L),
    a.name,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM
(
    SELECT DISTINCT
        UPPER(TRIM(name)) name
    FROM incoming.kom_d_subscription_group
) AS a
LEFT JOIN processed.d_vod_subscription_name AS b ON (UPPER(TRIM(a.name)) = UPPER(TRIM(b.subscription_name)))
WHERE
    b.subscription_name_id IS NULL;



------------------------------------------------------------------------------------------------------------------------
--
--   Stage 2
--
--   The customer information must not be kept un-encrypted and for this purpose, when the corp,
--   dwelling_nbr, cust columns are sourced from the kom_customer_account table in CVCDRP,
--   the chc_id created from these columns is encrypted.
--
--   To join the kom_customer_account with d_ecohort_chc and d_ethnic_chc, the chc_id from
--   these tables is also encrypted using the same encryption framework - CVSecurityApplication
--
--   Encryption is done using hive UDF function which uses the CVSecurityApplication.
--
--   If the table has multiple records for a single lookup key then the most recent record is
--   picked. This rule is applied to D_ECOHORT_CHC, D_ETHNIC_CHC, D_GEOG_CORP tables.
--   Staging tables are created to store the most recent records.
--
------------------------------------------------------------------------------------------------------------------------


--   Time taken by this query on dev cluster =

--  For Dev
--ADD jar hdfs://cvldhdpds1/appbis/app/SvodUsageDemographics/oozie/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar;
--  For Uat
ADD jar hdfs://cvhdpuat/app/util/SvodUsageDemographics/oozie/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar;
--  For Prod
--ADD jar hdfs://cvlphdpd1/app/util/SvodUsageDemographics/oozie/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar;

CREATE TEMPORARY function encrypt_chcid AS 'com.alticeusa.ds.svodusagedemographics.encryptor.EncryptFieldUDF';

TRUNCATE TABLE staging.encrypted_d_ecohort_chc;
set mapred.reduce.tasks=50;
set hive.execution.engine=tez;

-- kom_last_modified_date column is called kom_last_modified in dev - thanks to William

INSERT OVERWRITE TABLE staging.encrypted_d_ecohort_chc
SELECT DISTINCT
    chc_id,
    encrypt_chcid(chc_id) AS encrypted_chc_id,
    ecohort_code_id,
    from_month_id,
    to_month_id,
    kom_last_modified_date,
    rn
FROM
(
    SELECT
        chc_id,
        ecohort_code_id,
        from_month_id,
        to_month_id,
        kom_last_modified_date as kom_last_modified_date,
        ROW_NUMBER() OVER( PARTITION BY chc_id ORDER BY to_month_id DESC, kom_last_modified_date DESC) AS rn
    FROM processed.d_ecohort_chc
) AS t WHERE rn = 1;


--   Time taken by this query on dev cluster =


TRUNCATE TABLE staging.encrypted_d_ethnic_chc;
set mapred.reduce.tasks=50;
set hive.execution.engine=mr;

INSERT OVERWRITE TABLE staging.encrypted_d_ethnic_chc
SELECT DISTINCT
    chc_id,
    encrypt_chcid(chc_id) AS encrypted_chc_id,
    ethnic_code_id,
    from_month_id,
    to_month_id,
    kom_last_modified_date,
    rn
FROM
(
    SELECT
    chc_id,
    ethnic_code_id,
    from_month_id,
    to_month_id,
    kom_last_modified_date as kom_last_modified_date,
    ROW_NUMBER() OVER( PARTITION BY chc_id ORDER BY to_month_id DESC, kom_last_modified_date DESC) AS rn
    FROM processed.d_ethnic_chc
) t WHERE rn = 1;


--   Time taken by this query on dev cluster =


TRUNCATE TABLE staging.stg_d_geog_corp;

INSERT OVERWRITE TABLE staging.stg_d_geog_corp
SELECT
    corp_id,
    corp,
    end_date,
    rn
FROM
(
    SELECT
    corp_id,
    corp,
    end_date,
    ROW_NUMBER() OVER(PARTITION BY corp ORDER BY end_date DESC) AS rn
    FROM processed.d_geog_corp
) t WHERE rn = 1;


------------------------------------------------------------------------------------------------------------------------
--
--   Stage 3
--
--   Generate a staging table using the sourced tables ( given above) which will then be joined
--   with the dimension tables to replace dimension description by id.
--
--   The kom_f_vod_orders table is joined with the below tables to get the respective fields -
--
--   joined with kom_d_period - to retrieve the period_date
--   joined with kom_d_product_master - to retrieve the studio name, title name
--   joined with kom_d_subcription_group - to retrieve the subscription_name column
--
--   NOTE: the subscription_type column from the kom_d_subscription_group table is not used,
--   instead the vod_type column from vod_subscription_name_type_mapping table is used.
--
--   joined with kom_d_genre - to retrieve the genre name
--   joined with kom_vod_order - to retrieve the preview_ind, trailer_ind
--   joined with kom_customer_account - to retrieve corp, dwelling_nbr, cust fields which are
--   then used to create the chc_id which is futher encrypted using the CVSecurityApplication
--
--
------------------------------------------------------------------------------------------------------------------------


-- Time taken by this query on dev cluster =

TRUNCATE TABLE staging.stg1_vod_orders_mth_corp ;
set mapred.reduce.tasks=50;
set hive.execution.engine=mr;


INSERT OVERWRITE TABLE staging.stg1_vod_orders_mth_corp
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
    incoming.kom_f_vod_order a
    INNER JOIN incoming.kom_d_period b
    ON a.period_id = b.period_id
    INNER JOIN incoming.kom_d_product_master c
    ON a.product_master_id = c.product_master_id
    INNER JOIN incoming.kom_d_subscription_group d
    ON a.subscription_group_id = d.subscription_group_id
    INNER JOIN incoming.kom_d_genre e
    ON a.genre_id = e.genre_id
    INNER JOIN incoming.kom_vod_order f
    ON a.vod_order_id=f.vod_order_id
    INNER JOIN incoming.kom_customer_account g
    ON f.encrypted_customer_account_id=g.encrypted_customer_account_id
WHERE
    a.year_id=${YEAR_ID} AND a.month_id=${MONTH_ID} AND f.month_id=${YEAR_MONTH_ID};

------------------------------------------------------------------------------------------------------------------------

TRUNCATE TABLE staging.stg2_vod_orders_mth_corp;
set mapred.reduce.tasks=50;
set hive.execution.engine=mr;

INSERT OVERWRITE TABLE staging.stg2_vod_orders_mth_corp
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
FROM staging.stg1_vod_orders_mth_corp AS stg
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

------------------------------------------------------------------------------------------------------------------------
--
--   In this stage, the subscription_type is populated using the rules given in the BRD.
--
--   subscription_type = 'TRAILER', if preview_ind=1 or trailer_ind='Y'
--
--   subscription_type = 'SVOD', if on joining with vod_subscription_name_type_mapping table
--   on subscription_name the vod_type='SVOD'
--
--   subscription_type = 'FOD', if on join with vod_subscription_name_type_mapping table
--   the vod_type= 'FOD' or null but the vod_order_amt is 0
--
--   subscription_type = 'PVOD', if on join with vod_subscription_name_type_mapping table
--   the vod_type='FOD' or null but the vod_order_amt is not 0.
--
--   For the HD_FLAG column, if the title column contains '% HD' then the flag is set 'Y'
--   else 'N'

------------------------------------------------------------------------------------------------------------------------


--   Time taken by this query on dev cluster =


TRUNCATE TABLE staging.stg3_vod_orders_mth_corp;
set mapred.reduce.tasks=50;
set hive.execution.engine=mr;

INSERT OVERWRITE TABLE staging.stg3_vod_orders_mth_corp
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
FROM staging.stg2_vod_orders_mth_corp AS a
LEFT JOIN incoming.vod_subscription_name_type_mapping AS b
    ON UPPER(TRIM(a.subscription_name))=UPPER(TRIM(b.subscription_name));


------------------------------------------------------------------------------------------------------------------------
--
--   Stage 4
--
--   In this stage, the fact table is populated with aggregated data generated in the above stages
--   along with the dimension ids by joining with the dimension tables
--
--   For all the null dimensions, -1 is populated as default.
--
------------------------------------------------------------------------------------------------------------------------


--   Time taken by this query on dev cluster =


--TRUNCATE TABLE processed.f_vod_orders_mth_corp;
ALTER TABLE processed.f_vod_orders_mth_corp DROP IF EXISTS PARTITION(month_id=${YEAR_MONTH_ID});
set mapred.reduce.tasks=50;
set hive.execution.engine=mr;

INSERT INTO TABLE processed.f_vod_orders_mth_corp partition(month_id)
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
    stg.month_id
FROM staging.stg3_vod_orders_mth_corp AS stg
LEFT JOIN processed.d_vod_studio AS a
    ON (UPPER(stg.studio_desc)=UPPER(a.studio_desc))
LEFT JOIN processed.d_vod_title AS b
    ON (UPPER(stg.title_desc)=UPPER(b.title_desc))
LEFT JOIN processed.d_vod_subscription_name AS c
    ON (UPPER(TRIM(stg.subscription_name))=UPPER(TRIM(c.subscription_name)))
LEFT JOIN processed.d_vod_subscription_type AS d
    ON (UPPER(TRIM(stg.subscription_type))=UPPER(TRIM(d.subscription_type)))
LEFT JOIN processed.d_vod_genre AS e
    ON (UPPER(stg.genre_desc)=UPPER(e.genre_desc))
INNER JOIN staging.stg_d_geog_corp AS f
    ON (stg.corp=f.corp)
INNER JOIN staging.encrypted_d_ecohort_chc AS g
    ON (stg.encrypted_chc_id=g.encrypted_chc_id)
INNER JOIN staging.encrypted_d_ethnic_chc AS h
    ON (stg.encrypted_chc_id=h.encrypted_chc_id);


------------------------------------------------------------------------------------------------------------------------
--
--   Finished
--
------------------------------------------------------------------------------------------------------------------------
