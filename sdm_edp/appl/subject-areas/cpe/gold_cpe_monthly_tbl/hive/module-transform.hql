--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive script
--#   Purpose:    : Deriving the gold_cpe_monthly_tbl table from gold tables.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 01/23/2017
--#   Log File    : .../log/cpe/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/cpe/${job_name}.log
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

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_cpe_monthly_table}  PARTITION(P_YYYYMM)
SELECT CONCAT(substr('${hivevar:begin_date}',1,4), '-', substr('${hivevar:begin_date}',5,2), '-', substr('${hivevar:begin_date}',7,2)) BEGIN_date  
     , CONCAT(substr('${hivevar:end_date}',1,4), '-', substr('${hivevar:end_date}',5,2), '-', substr('${hivevar:end_date}',7,2))  END_date
     , a.region_ps_id   
     , a.plant_manager_descr  
     , a.head_end_descr       
     , a.node_cd            
     , a.item_nbr             
     , a.eqmt_type            
     , ROUND(a.customer_count_avg)   customer_count_avg
     , ROUND(a.equipment_count_avg)  equipment_count_avg
     , a.ivr_tech_total
     , a.ivr_tech_contain
     , a.ivr_tech_offer
     , a.ivr_tech_video_total
     , a.ivr_tech_video_contain
     , a.ivr_tech_video_offer
     , a.ivr_tech_data_total
     , a.ivr_tech_data_contain
     , a.ivr_tech_data_offer
     , a.ivr_tech_phone_total
     , a.ivr_tech_phone_contain
     , a.ivr_tech_phone_offer
     , a.wo_tc_video_total   
     , a.wo_tc_data_total     
     , a.wo_tc_phone_total   
     , a.WO_TC_TOTAL 
      --
     , b.Model  
     , b.Model_Desc
     , b.LOB 
     , b.Category 
     , b.Make 
     , b.Active 
     , b.RYG_Status 
     , b.CPE_Forecast_Category 
     , b.M2_or_M4 
     , b.Model_Trim  
     , b.Model_Category
     , b.Docsis    
     --
     , substr('${hivevar:begin_date}', 1, 6)  as  P_YYYYMM
FROM (
        SELECT REGION_PS_ID
             , PLANT_MANAGER_DESCR
             , HEAD_END_DESCR
             , node_cd
             , ITEM_NBR
             , EQMT_TYPE        
             , AVG(cust_count)     customer_count_avg
             , AVG(eqmt_count)     equipment_count_avg
             --
             , SUM(ivr_tech_total)          ivr_tech_total
             , SUM(ivr_tech_contain)        ivr_tech_contain
             , SUM(ivr_tech_offer)          ivr_tech_offer
             , SUM(ivr_tech_video_total)    ivr_tech_video_total
             , SUM(ivr_tech_video_contain)  ivr_tech_video_contain
             , SUM(ivr_tech_video_offer)    ivr_tech_video_offer
             , SUM(ivr_tech_data_total)     ivr_tech_data_total
             , SUM(ivr_tech_data_contain)   ivr_tech_data_contain
             , SUM(ivr_tech_data_offer)     ivr_tech_data_offer
             , SUM(ivr_tech_phone_total)    ivr_tech_phone_total
             , SUM(ivr_tech_phone_contain)  ivr_tech_phone_contain
             , SUM(ivr_tech_phone_offer)    ivr_tech_phone_offer
             --
             , SUM(WO_TC_VIDEO)    WO_TC_VIDEO_TOTAL
             , SUM(WO_TC_DATA)     WO_TC_DATA_TOTAL
             , SUM(WO_TC_PHONE)    WO_TC_PHONE_TOTAL
             , SUM(WO_TC_TOTAL)    WO_TC_TOTAL
        FROM ${hivevar:gold_database}.${hivevar:table_prefix}${hivevar:gold_cpe_daily_table}
        WHERE 1 = 1
        AND p_yyyymmdd between '${hivevar:begin_date}' and '${hivevar:end_date}'
        GROUP BY REGION_PS_ID
             , PLANT_MANAGER_DESCR
             , HEAD_END_DESCR
             , node_cd
             , ITEM_NBR
             , EQMT_TYPE 
    ) a  
LEFT OUTER JOIN ${hivevar:incoming_database}.${hivevar:table_prefix}${hivevar:incoming_cpe_model_info_table} b ON (a.ITEM_NBR = b.Model)
;

--##############################################################################
--#                                    End                                     #
--##############################################################################