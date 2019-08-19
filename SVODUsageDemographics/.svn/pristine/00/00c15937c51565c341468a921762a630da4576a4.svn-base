------------------------------------------------------------------------------------------------------------------------
--
--   Program name: svodusagedemo_rollback.sql
--   Program type: sql script
--   Author      : Kriti Singh
--   Date        : 05/29/2016
--
--   Description : This sql script rolls back the changes made in the redshift prod database
--
--
------------------------------------------------------------------------------------------------------------------------

--Delete the entries from the aws_job_master table

delete from aws_job_master where aws_job_id=100018;
delete from aws_job_master where aws_job_id=100019;
delete from aws_job_master where aws_job_id=100020;
delete from aws_job_master where aws_job_id=100021;
delete from aws_job_master where aws_job_id=100022;

--Drop the vod tables
DROP TABLE IF EXISTS d_vod_title CASCADE;
DROP TABLE IF EXISTS d_vod_studio CASCADE;
DROP TABLE IF EXISTS d_vod_genre CASCADE;
DROP TABLE IF EXISTS d_vod_subscription_name CASCADE;
DROP TABLE IF EXISTS d_vod_subscription_type CASCADE;
DROP TABLE IF EXISTS f_vod_orders_mth_corp CASCADE;

