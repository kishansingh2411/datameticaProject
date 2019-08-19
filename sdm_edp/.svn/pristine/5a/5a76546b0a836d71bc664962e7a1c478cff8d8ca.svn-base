--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_equipment table from gold.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 01/23/2017
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
--#    1.0     DataMetica Team          01/23/2017       Initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_equipment_table} 
PARTITION(P_YYYYMMDD)
SELECT 
       e_dtls.SITE_ID             as SITE_ID
     , e_dtls.ITEM_NBR            as ITEM_NBR
     , e_dtls.SERIAL_NBR          as SERIAL_NBR
     , e_dtls.PORT_NBR            as PORT_NBR
     , e_dtls.ACCT_NBR            as ACCT_NBR
     , e_dtls.EQUIPMENT_ADDRESS   as EQUIPMENT_ADDRESS
     , e_dtls.IP_ADDRESS          as IP_ADDRESS
     , e_dtls.SERVICE_OCCURRENCE  as SERVICE_OCCURRENCE
     , e_dtls.eqmt_type           as eqmt_type
     , e_dtls.mac_address         as mac_address
     , i_mstr.ITEM_DESC           as ITEM_DESC
     , i_mstr.ITEM_MODEL_NBR      as ITEM_MODEL_NBR
     , i_mstr.ITEM_MODEL_MAKE     as ITEM_MODEL_MAKE
     , i_mstr.ITEM_DOCSIS_VER     as ITEM_DOCSIS_VER
     , i_mstr.VENDOR              as VENDOR
     , e_dtls.SRVC_CATG_CD        as SRVC_CATG_CD
     , e_dtls.PORT_TYPE           as PORT_TYPE
     , i_dtls.ITEM_DESC           as ITEM_DTL_DESC
     , i_dtls.PORT_CATG_CD        as PORT_CATG_CD
     , NVL(e_actv.ACTIVE_SRV_FLAG, 0)    as ACTIVE_SRV_FLAG 
     , NVL(e_actv.active_svc_ctgy_c, 0)  as active_svc_ctgy_c 
     , NVL(e_actv.active_svc_ctgy_d, 0)  as active_svc_ctgy_d 
     , NVL(e_actv.active_svc_ctgy_h, 0)  as active_svc_ctgy_h 
     , NVL(e_actv.active_svc_ctgy_s, 0)  as active_svc_ctgy_s 
     , NVL(e_actv.active_svc_ctgy_t, 0)  as active_svc_ctgy_t 
     , NVL(e_actv.lob_data, 0)           as lob_data 
     , NVL(e_actv.lob_home, 0)           as lob_home 
     , NVL(e_actv.lob_cable, 0)          as lob_cable 
     , NVL(e_actv.lob_phone, 0)          as lob_phone 
     , cust.REGION_DESCR  
     , cust.REGION_PS_ID  
     , cust.BUSINESS_UNIT_DESCR 
     , cust.BUSINESS_UNIT_PS_ID
     , cust.BUDGET_ENTITY_DESCR 
     , cust.BUDGET_ENTITY_PS_ID 
     , cust.PLANT_MANAGER_DESCR  
     , cust.PLANT_MANAGER_PS_ID  
     , cust.HEAD_END_DESCR 
     , cust.HEAD_END_PS_ID     
     , cust.cust_group_cd
     , cust.HSD_USAGE_RPT_FLAG  
     , cust.company_nbr
     , cust.division_nbr
     , cust.headend    
     , cust.node_cd
     , cust.LONGT_NO
     , cust.lat_no
     , cust.house_nbr 
     , cust.addr_loc
     , cust.addr_fraction 
     , cust.pre_directional 
     , cust.addl_post_dir 
     , cust.street_nm 
     , cust.city_nm 
     , cust.state_cd
     , cust.us_zip5 
     , cust.us_zip4 
     , cust.bldg 
     , cust.apt 
     , ''  CMTS     
     , '${hivevar:source_date}' p_yyyymmdd
FROM (
        SELECT P_YYYYMMDD
             , SITE_ID
             , ITEM_NBR
             , SERIAL_NBR
             , PORT_NBR
             , EQUIPMENT_ADDRESS
             , IP_ADDRESS
             , ACCT_NBR
             , PORT_TYPE
             , SERVICE_CAT_CD as SRVC_CATG_CD
             , SERVICE_OCCURRENCE
             , EQMT_TYPE
             , MAC_ADDRESS
        FROM (
                -- NOTE: There are duplicate rows in sdm.equipment_dtls. So, use RANK to eliminate the dups.
                SELECT P_YYYYMMDD
                     , SITE_ID
                     , ITEM_NBR
                     , SERIAL_NBR
                     , PORT_NBR
                     , EQUIPMENT_ADDRESS
                     , IP_ADDRESS
                     , ACCT_NBR
                     , PORT_TYPE
                     , SERVICE_CAT_CD
                     , SERVICE_OCCURRENCE
                     , (CASE WHEN PORT_TYPE IN ('TELEPHONY', 'TELEPHONE') THEN 'PHONE' WHEN PORT_TYPE IN ('DATA') THEN 'INTERNET' ELSE 'VIDEO' END) as EQMT_TYPE
                     , ''   as MAC_ADDRESs
                     , ROW_NUMBER() OVER (PARTITION BY SITE_ID, ITEM_NBR, SERIAL_NBR, PORT_TYPE, PORT_NBR ORDER BY P_YYYYMMDD DESC, EQUIPMENT_DTLS_KEY DESC) AS rank_nbr
                FROM ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_equipment_dtls_table} 
                WHERE 
                	p_yyyymmdd = '${hivevar:source_date}'
              ) a
        WHERE a.rank_nbr = 1 -- remove duplicate rows
     ) e_dtls
     INNER JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_item_master_table} i_mstr
          ON (i_mstr.P_YYYYMMDD  = '${hivevar:source_date}' AND i_mstr.site_id  = e_dtls.site_id  AND i_mstr.item_nbr = e_dtls.item_nbr)
     INNER JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_item_dtl_table} i_dtls
          ON (i_dtls.P_YYYYMMDD   = '${hivevar:source_date}' AND i_dtls.site_id   = e_dtls.site_id AND i_dtls.ITEM_NBR  = e_dtls.ITEM_NBR AND i_dtls.PORT_TYPE = e_dtls.PORT_TYPE AND i_dtls.PORT_NBR  = e_dtls.PORT_NBR)
     LEFT OUTER JOIN ${hivevar:work_database}.${hivevar:table_prefix}${hivevar:work_acct_srv_occur_lob_table} e_actv    
          ON (e_dtls.site_id  = e_actv.site_id AND e_dtls.acct_nbr = e_actv.acct_nbr AND e_dtls.srvc_catg_cd = e_actv.svc_ctgy_cd AND e_dtls.service_occurrence = e_actv.service_occurrence)     
     INNER JOIN ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_customer_table} cust 
     	  ON (cust.site_id = e_dtls.site_id AND cust.acct_nbr = e_dtls.acct_nbr)             
WHERE 1 = 1
;

--##############################################################################
--#                                    End                                     #
--##############################################################################