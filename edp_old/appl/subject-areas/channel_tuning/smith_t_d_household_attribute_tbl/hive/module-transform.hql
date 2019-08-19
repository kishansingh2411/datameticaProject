--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Build Gold table from Incoming table
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module-transform.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/channel_tuning/
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################

INSERT OVERWRITE 
   TABLE ${hivevar:target_hive_database}.${hivevar:target_table}
   SELECT
	 household_attr_id ,
	 household_id ,
	 dtm_created ,
	 dtm_last_updated ,
	 created_by ,
	 last_updated_by ,
	 mflag ,
	 corp ,
	 cohort ,
	 cohort_text ,
	 ethnic ,
	 language ,
	 groupcode ,
	 country ,
	 cust_res ,
	 enhanced_est_income,
	 i1_combined_age ,
	 i1_gendercode ,
	 household_composition ,
	 dwelling_type ,
	 length_of_residence ,
	 number_of_children ,
	 i2_exact_age ,
	 i1_occupation ,
	 occupation_group1 ,
	 education_individual1 ,
	 product_id ,
	 mdm_tier_rpt ,
	 zipcode ,
	 product_code ,
	 ethnic1 ,
	 lnguage ,
	 country1 ,
	 group1 ,
	 dwellt ,
	 lnth_res ,
	 income ,
	 gend ,
	 age1 ,
	 accultur ,
	 acculturization1 ,
	 household_key2 ,
	 recordind ,
	 tgt_dtm_created ,
	 tgt_dtm_last_updated
FROM 
	 ${hivevar:src_hive_database}.${hivevar:src_table}
;

--##############################################################################
--#                                    End                                     #
--##############################################################################