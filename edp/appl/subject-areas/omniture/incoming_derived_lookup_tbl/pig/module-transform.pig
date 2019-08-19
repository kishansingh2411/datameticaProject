--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module-transform.pig
--#   Program type: Pig script
--#   Purpose:    : Deriving the incoming derived lookup table from incoming_hit_data.
--#   Author(s)   : DataMetica Team
--#   Usage       : pig -p var1=var1 -useHCatalog module-transform.pig
--#   Date        : 12/28/2015
--#   Log File    : .../log/${suite_name}/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${suite_name}/${job_name}.log
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

--##############################################################################
--#                           Hive Table Definition                            #
--##############################################################################

import '${LOOKUP_MACROS_SCRIPT_ABSOLUTE_PATH}';

-- Loading tables

incoming_hit_data =
   LOAD '${source_database}.${source_table}'
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_optimum_isp = 
   LOAD '${source_database}.${isp_table}'
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_optimum_site_section = 
   LOAD '${source_database}.${site_section_table}'
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

incoming_optimum_pagenames = 
   LOAD '${source_database}.${pagenames_table}'
   USING org.apache.hive.hcatalog.pig.HCatLoader()
;

-- Filtering tables

curr_incoming_hit_data =
   FILTER incoming_hit_data BY (suite_name == '${suite_name}') 
   AND (source_date == '${source_date}')
;

suite_incoming_optimum_isp =
   FILTER incoming_optimum_isp BY (suite_id == '${suite_id}')
;

suite_incoming_optimum_site_section =
   FILTER incoming_optimum_site_section BY (suite_id == '${suite_id}')
;

suite_incoming_optimum_pagenames =
   FILTER incoming_optimum_pagenames BY (suite_id == '${suite_id}')
;

--# Deriving distinct ids for lookup table

not_null_distinct_isp_names_hit_data = 
   derive_lookup (curr_incoming_hit_data, 'domain', 'isp_name')
;

not_null_distinct_site_section_hit_data =
   derive_lookup (curr_incoming_hit_data, 'channel', 'site_section_name')
;

not_null_distinct_pagename_hit_data =
   derive_lookup (curr_incoming_hit_data, 'evar1', 'pagename')
;

--# Joining with distinct datasets

incoming_optimum_isp_hit_data_join =
   JOIN not_null_distinct_isp_names_hit_data BY isp_name left, 
   suite_incoming_optimum_isp BY isp_name
   USING 'replicated'
; 

incoming_optimum_site_section_hit_data_join =
   JOIN not_null_distinct_site_section_hit_data BY site_section_name left,
   suite_incoming_optimum_site_section BY site_section_name
   USING 'replicated'
;

incoming_optimum_pagenames_hit_data_join =
   JOIN not_null_distinct_pagename_hit_data BY pagename left,
   suite_incoming_optimum_pagenames BY pagename
   USING 'replicated'
;

--# Deriving new values from joined datasets

incoming_optimum_new_isps =
   FILTER incoming_optimum_isp_hit_data_join BY 
   (suite_incoming_optimum_isp::isp_name is null)
;

incoming_optimum_new_site_section =
   FILTER incoming_optimum_site_section_hit_data_join BY 
   (suite_incoming_optimum_site_section::site_section_name is null)
;

incoming_optimum_new_pagenames =
   FILTER incoming_optimum_pagenames_hit_data_join BY 
   (suite_incoming_optimum_pagenames::pagename is null)
;

--# Adding ids to newly derived datasets

rank_incoming_optimum_new_isps = RANK incoming_optimum_new_isps BY 
   not_null_distinct_isp_names_hit_data::isp_name asc
;

rank_incoming_optimum_new_site_section = RANK incoming_optimum_new_site_section 
   BY not_null_distinct_site_section_hit_data::site_section_name asc
;

rank_incoming_optimum_new_pagenames = RANK incoming_optimum_new_pagenames BY 
   not_null_distinct_pagename_hit_data::pagename asc
;
 
--# Deriving unique ids for newly derived lookup values

incoming_optimum_isp_out = 
   FOREACH rank_incoming_optimum_new_isps GENERATE 
   (bigdecimal)rank_incoming_optimum_new_isps + ${max_isp_id} as isp_id,
   not_null_distinct_isp_names_hit_data::isp_name as isp_name,
   '${source_date}' as dtm_start,
   'omniture' as sourced_from_system,
   CurrentTime() as dtm_created   
;

incoming_optimum_site_section_out = 
   FOREACH rank_incoming_optimum_new_site_section GENERATE 
   (bigdecimal)rank_incoming_optimum_new_site_section + ${max_site_section_id} as site_section_id,
   not_null_distinct_site_section_hit_data::site_section_name as site_section_name,
   '${source_date}' as dtm_start,
   'omniture' as sourced_from_system,
   CurrentTime() as dtm_created   
;

incoming_optimum_pagenames_out = 
   FOREACH rank_incoming_optimum_new_pagenames GENERATE 
   (bigdecimal)rank_incoming_optimum_new_pagenames + ${max_pagenames_id} as page_name_id,
   not_null_distinct_pagename_hit_data::pagename as pagename,
    '${source_date}' as dtm_start,
   'omniture' as sourced_from_system,
   CurrentTime() as dtm_created   
;

--# Storing new datasets in tables

STORE incoming_optimum_isp_out 
   INTO '${source_database}.${isp_table}'
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_id=${suite_id},source_date=${source_date}')
;

STORE incoming_optimum_site_section_out
   INTO '${source_database}.${site_section_table}'
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_id=${suite_id},source_date=${source_date}')
;

STORE incoming_optimum_pagenames_out 
   INTO '${source_database}.${pagenames_table}'
   USING org.apache.hive.hcatalog.pig.HCatStorer('suite_id=${suite_id},source_date=${source_date}')
;

--##############################################################################
--#                                    End                                     #
--##############################################################################