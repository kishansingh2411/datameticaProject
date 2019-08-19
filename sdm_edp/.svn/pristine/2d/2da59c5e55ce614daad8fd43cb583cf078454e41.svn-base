--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the work work_acct_srv_occur_lob table from gold.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
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
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          01/22/2017       Initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE TABLE ${hivevar:work_database}.${hivevar:table_prefix}${hivevar:work_acct_srv_occur_lob_table} 
SELECT 
	srv.SITE_ID
     , srv.ACCT_NBR
     , cust.cust_group_cd
     , srv.service_occurrence
     , srv.svc_ctgy_cd
     , MAX(CASE WHEN srv.service_status = 'A'  THEN 1 ELSE 0 END) active_srv_flag
     , MAX(CASE WHEN srv.SVC_CTGY_CD = 'C' AND srv.service_status = 'A' THEN 1 ELSE 0 END) active_svc_ctgy_c
     , MAX(CASE WHEN srv.SVC_CTGY_CD = 'D' AND srv.service_status = 'A' THEN 1 ELSE 0 END) active_svc_ctgy_d
     , MAX(CASE WHEN srv.SVC_CTGY_CD = 'H' AND srv.service_status = 'A' THEN 1 ELSE 0 END) active_svc_ctgy_h
     , MAX(CASE WHEN srv.SVC_CTGY_CD = 'S' AND srv.service_status = 'A' THEN 1 ELSE 0 END) active_svc_ctgy_s
     , MAX(CASE WHEN srv.SVC_CTGY_CD = 'T' AND srv.service_status = 'A' THEN 1 ELSE 0 END) active_svc_ctgy_t
     --
     , MAX(CASE 
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'RESI' 
                 AND dim.sub_count_flag = 'Y' 
                 --AND dim.srv_lob_dim_pk = 1 
                 AND dim.srv_group_dim_pk IN (12) --HSD
                 THEN 
                    1
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'COMM' 
                 AND dim.com_sub_count_flag = 'Y' 
                 --AND dim.srv_com_lob_dim_pk = 1 
                 AND dim.srv_com_group_dim_pk IN (27, 38, 42) -- INTERNET
                 THEN 
                    1
                ELSE 
                    0
        END) lob_data
     , MAX(CASE 
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'RESI' 
                 AND dim.sub_count_flag = 'Y' 
                 --AND dim.srv_lob_dim_pk = 2
                 AND dim.srv_group_dim_pk IN (2) -- Home Security
                 THEN 
                    1
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'COMM' 
                 AND dim.com_sub_count_flag = 'Y' 
                 --AND dim.srv_com_lob_dim_pk = 2
                 AND dim.srv_com_group_dim_pk IN (2) -- Home Security 
                 THEN 
                    1
                ELSE 
                    0
       END) lob_home
     , MAX(CASE 
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'RESI' 
                 AND dim.sub_count_flag = 'Y' 
                 --AND dim.srv_lob_dim_pk = 4 
                 AND dim.srv_group_dim_pk IN (1, 4, 17) -- Basic, Expanded, Digital
                 THEN 
                    1
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'COMM' 
                 AND dim.com_sub_count_flag = 'Y' 
                 --AND dim.srv_com_lob_dim_pk = 4
                 AND dim.srv_com_group_dim_pk IN (1, 4, 17) -- Basic, Expanded, Digital
                 THEN 
                    1
                ELSE 
                    0
       END) lob_cable
     , MAX(CASE 
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'RESI' 
                 AND dim.sub_count_flag = 'Y' 
                 --AND dim.srv_lob_dim_pk = 5
                 AND dim.srv_group_dim_pk IN (20) -- Telephone
                 THEN 
                    1
                WHEN srv.service_status = 'A' 
                 AND cust.cust_group_cd = 'COMM' 
                 AND dim.com_sub_count_flag = 'Y' 
                 --AND dim.srv_com_lob_dim_pk = 5
                 AND dim.srv_com_group_dim_pk IN (20) -- Telephone
                 THEN 
                    1
                ELSE 
                    0
       END) lob_phone
     , srv.P_YYYYMMDD 
FROM ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_customer_services_table} srv
INNER JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_customer_table} cust 
        ON (cust.site_id = srv.site_id AND cust.acct_nbr = srv.acct_nbr)
INNER JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_srv_dim_table} dim
        ON (dim.p_yyyymmdd = '${hivevar:source_date}'  AND dim.srv_code = srv.service_cd AND dim.rec_status = 'C')
WHERE srv.P_YYYYMMDD = '${hivevar:source_date}' 
GROUP BY srv.P_YYYYMMDD, srv.SITE_ID, srv.ACCT_NBR, cust.cust_group_cd, srv.svc_ctgy_cd, srv.service_occurrence 
;

--##############################################################################
--#                                    End                                     #
--##############################################################################