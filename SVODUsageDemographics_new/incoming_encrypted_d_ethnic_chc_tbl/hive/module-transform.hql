--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the incoming_encrypted_d_ethnic_chc table from gold.
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

-- ADD jar hdfs://althdpdev/app/util/SvodUsageDemographics/oozie/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar;
ADD jar ${hivevar:jar_path}/SVODUsageDemographics-1.0-SNAPSHOT.jar;
CREATE TEMPORARY function encrypt_chcid AS 'com.alticeusa.ds.svodusagedemographics.encryptor.EncryptFieldUDF';

-- TRUNCATE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table};
set mapred.reduce.tasks=100;
set hive.execution.engine=tez;

-- kom_last_modified_date column is called kom_last_modified in dev - thanks to William

INSERT OVERWRITE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
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
    FROM ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
) t WHERE rn = 1;

