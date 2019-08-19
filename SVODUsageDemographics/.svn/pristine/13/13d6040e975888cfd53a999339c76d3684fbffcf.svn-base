----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--   Program name: dimension_table_1_time_population.hql
--   Program type: Hive script
--   Author      : Kriti Singh
--   Date        : 05/11/2016
--
--   Description : This hive script populates the dimension tables for the first time.
--
--
--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--  UAT
--add jar hdfs://cvhdpuat/app/util/SvodUsageDemographics/oozie/lib/cvc-hive-udf-0.0.2.jar;
--  DEV
--add jar hdfs://cvldhdpds1/appbis/app/SvodUsageDemographics/oozie/lib/cvc-hive-udf-0.0.2.jar;
--  PROD
add jar hdfs://cvlphdpd1/app/util/SvodUsageDemographics/oozie/lib/cvc-hive-udf-0.0.2.jar;

create temporary function delete_hl_seq as 'com.cvc.udf.impl.HLSequenceCleaner';
--create temporary function gen_incr as 'com.cvc.udf.imp.HLSequenceGenerator';


select delete_hl_seq("studio_id");
select delete_hl_seq("title_id");
select delete_hl_seq("subscription_name_id");
select delete_hl_seq("genre_id");


DROP TABLE staging.tmp_distinct_studio;

CREATE TABLE staging.tmp_distinct_studio as
SELECT distinct
    (upper(trim(studio))) AS studio
FROM incoming.kom_d_product_master;



TRUNCATE TABLE processed.d_vod_studio;

INSERT INTO TABLE processed.d_vod_studio
SELECT
    seq_gen('studio_id',1000,1L),
    studio,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM staging.tmp_distinct_studio;

INSERT INTO TABLE processed.d_vod_studio
SELECT
    -1,
    'N/A',
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM processed.d_vod_studio
LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


DROP TABLE staging.tmp_distinct_title;

CREATE TABLE staging.tmp_distinct_title AS
SELECT DISTINCT
    (UPPER(TRIM(title))) AS title
FROM incoming.kom_d_product_master;




TRUNCATE TABLE processed.d_vod_title;

INSERT INTO TABLE processed.d_vod_title
SELECT
    seq_gen('title_id',1000,1L),
    title,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM staging.tmp_distinct_title;

INSERT INTO TABLE processed.d_vod_title
SELECT
    -1,
    'N/A',
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM processed.d_vod_title
LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


DROP TABLE staging.tmp_distinct_subscription_name;

CREATE TABLE staging.tmp_distinct_subscription_name as
SELECT DISTINCT UPPER(TRIM(name)) AS name
FROM incoming.kom_d_subscription_group;




TRUNCATE TABLE processed.d_vod_subscription_name;

INSERT INTO TABLE processed.d_vod_subscription_name
SELECT
    seq_gen('subscription_name_id',1000,1L),
    name,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM staging.tmp_distinct_subscription_name;


INSERT INTO TABLE processed.d_vod_subscription_name
SELECT
    -1,
    'N/A',
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM processed.d_vod_subscription_name
LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



DROP TABLE staging.tmp_distinct_genre;

CREATE TABLE staging.tmp_distinct_genre AS
SELECT DISTINCT
    (UPPER(TRIM(name))) AS name
FROM incoming.kom_d_genre;



TRUNCATE TABLE processed.d_vod_genre;

INSERT INTO TABLE processed.d_vod_genre
SELECT
    seq_gen('genre_id',1000,1L),
    name,
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM staging.tmp_distinct_genre;

INSERT INTO TABLE processed.d_vod_genre
SELECT
    -1,
    'N/A',
    FROM_UNIXTIME(UNIX_TIMESTAMP()),
    FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM processed.d_vod_genre
LIMIT 1;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

