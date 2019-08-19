--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_encrypted_d_ecohort_chc_tbl table from incoming.
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



-- TRUNCATE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table};

INSERT OVERWRITE TABLE ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
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
    FROM  ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
) t WHERE rn = 1;
