--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_d_vod_studio table from incoming.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 03/22/2017
--#   Log File    : .../log/svod/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/svod/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#    1.0     DataMetica Team          03/22/2017       Initial version
--#
--#
--#####################################################################################################################

INSERT INTO ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table}
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
    SELECT DISTINCT UPPER(TRIM(studio)) AS studio FROM ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_table}
) AS a
LEFT JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_table} AS b
    ON (UPPER(TRIM(a.studio)) = UPPER(TRIM(b.studio_desc)))
WHERE 
	b.studio_id IS NULL
;

--##############################################################################
--#                                    End                                     #
--##############################################################################