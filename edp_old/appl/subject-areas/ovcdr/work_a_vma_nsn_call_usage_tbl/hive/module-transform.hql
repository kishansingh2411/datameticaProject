--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Work table from GOLD table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2016
--#   Log File    : .../log/ovcdr/{job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/ovcdr/{job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2016       Initial version
--#
--#
--#####################################################################################################################

set hive.exec.dynamic.partition.mode=nonstrict
;

FROM(
 SELECT
		unall.usage_date,
		unall.telephone_number,
		unall.on_net_ind,
		unall.number_of_calls,
		unall.batch_nbr,
		unall.attempt_count,
		unall.corp,
		unall.house,
		unall.cust,
		unall.dtm_created,
		unall.dtm_last_modified,
		unall.file_name		
 FROM 
(
		SELECT
			src_gold.usage_date,
			src_gold.telephone_number,
			src_gold.on_net_ind,
			src_gold.number_of_calls,
			src_gold.batch_nbr,
			src_gold.attempt_count,
			src_work.corp,
			src_work.house,
			src_work.cust,
			src_gold.dtm_created,
			current_timestamp() as dtm_last_modified,
			'${hivevar:file_name}' as file_name
			
		FROM 
			${hivevar:gold_database}.${hivevar:gold_table} src_gold
		LEFT OUTER JOIN 
		    ${hivevar:work_database}.${hivevar:work_chc_table} src_work 
		ON 
		    (src_gold.usage_date=src_work.usage_date and src_gold.telephone_number = src_work.telephone_number)
		WHERE 
		    to_date(src_gold.dtm_created) >= to_date(date_sub('${hivevar:source_date}',60)) and src_gold.corp = '-1'
 UNION ALL
		SELECT  
			gold.usage_date,
			gold.telephone_number,
			gold.on_net_ind,
			gold.number_of_calls,
			gold.batch_nbr,
			gold.attempt_count,
			gold.corp,
			gold.house,
			gold.cust,
			gold.dtm_created,
			gold.dtm_last_modified,
		    '${hivevar:file_name}' as file_name
		   
		FROM 
			${hivevar:gold_database}.${hivevar:gold_table} gold
		WHERE 
		   to_date(gold.dtm_created) >= to_date(date_sub('${hivevar:source_date}',60)) and gold.corp !='-1'
 ) unall
 
 ) insertwork
 
INSERT INTO TABLE 
   ${hivevar:work_database}.${hivevar:work_table}
   SELECT 
        insertwork.usage_date,
        insertwork.telephone_number,
		insertwork.on_net_ind,
		insertwork.number_of_calls,
		insertwork.batch_nbr,
		insertwork.attempt_count,
		insertwork.corp,
		insertwork.house,
		insertwork.cust,
		insertwork.dtm_created,
		insertwork.dtm_last_modified,
		insertwork.file_name	
;


--##############################################################################
--#                                    End                                     #
--##############################################################################