--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_page_view      								   		           #
--# File                                                                       #
--#     : work_page_view.pig                                   	               #
--# Description                                                                #
--#     : To load data into work_page_view table.              	               #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Vikram Chaudhari/Mikhil Nagarale                         			   #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$WORK_PAGE_VIEW_SSH_ACTION';
SET default_parallel 2;

--Loading data from gold_clickstream.page_view table.
work_page_view = 
   LOAD '$DB_GOLD.$GOLD_PAGE_VIEW'
   USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id and not null session_id.
work_page_view_filter = 
   FILTER work_page_view BY (batch_id=='$batch_id' AND session_id IS NOT NULL);

-- Selecting required fields from table.
-- If value of country will be null it will be replaced with 'DEFAULT'.   
work_page_view_generate = 
   FOREACH work_page_view_filter GENERATE
   session_id AS session_id,
   visitor_segment AS visitor_segment,
   page_id AS page_id,
   timestamp AS load_date,
   (country_description IS NULL OR country_description=='' OR country_description==' ' ? '$DEF_VALUE':country_description) AS country_description;   

-- Grouping total page views records by load_date and country_description.
work_page_view_page_group = 
   GROUP work_page_view_generate BY (load_date,country_description,visitor_segment);

-- Generating total_page_view records by date and country_description.
work_page_view_page_group_aggregate = 
   FOREACH work_page_view_page_group GENERATE 
   group, COUNT_STAR(work_page_view_generate.page_id) AS total_page_view;

-- Flattening records.
-- $0 will generate load_date and country.
-- $1 will genetare total_page_views.
work_page_view_page_flatten = 
   FOREACH work_page_view_page_group_aggregate GENERATE FLATTEN(group),total_page_view;

-- Generating records for total page views.
work_page_view_generate_page_flatten = 
   FOREACH work_page_view_page_flatten 
  GENERATE load_date AS load_date, country_description AS country_description, visitor_segment AS visitor_segment ,total_page_view AS total_page_view;

-- Selecting country and session_id.
work_page_view_generate_session = 
   FOREACH work_page_view_generate 
   GENERATE country_description AS country_description, session_id AS session_id,visitor_segment AS visitor_segment;

-- Selecting distinct records for finding total unique visitors.   
work_page_view_generate_session_distinct = 
   DISTINCT work_page_view_generate_session;

-- Grouping records by country_description.
work_page_view_session_group = 
   GROUP work_page_view_generate_session_distinct BY (country_description,visitor_segment);

-- Generating total unique visitor records by country_description.
work_page_view_session_group_aggregate = 
   FOREACH work_page_view_session_group GENERATE FLATTEN(group) as (country_description,visitor_segment),
   COUNT_STAR(work_page_view_generate_session_distinct.session_id) AS unique_session_id; 

-- joining alias work_page_view_generate_page_flatten and work_page_view_session_group_aggregate.
work_page_view_session_page_join =
   JOIN work_page_view_generate_page_flatten BY (country_description,visitor_segment) FULL OUTER,
   work_page_view_session_group_aggregate BY (country_description,visitor_segment);

-- Generating records.
--generate_records = 
--   FOREACH work_page_view_session_page_join GENERATE
--   $1 AS country_description,
--   $2 AS total_page_view,
--   $4 AS unique_session_id,
--   $0 AS load_date,
--   '$batch_id' AS batch_id;

generate_records = 
  FOREACH work_page_view_session_page_join GENERATE
   work_page_view_generate_page_flatten::country_description AS country_description,
   work_page_view_generate_page_flatten::visitor_segment AS visitor_segment,
   work_page_view_generate_page_flatten::total_page_view AS total_page_view,
   work_page_view_session_group_aggregate::unique_session_id AS unique_session_id,
   work_page_view_generate_page_flatten::load_date AS load_date,
   '$batch_id' AS batch_id;
         
--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing Data in work_clickstream.wokr_page_view table.
STORE generate_records 
   INTO '$DB_WORK.$WORK_PAGE_VIEW' 
   USING org.apache.hive.hcatalog.pig.HCatStorer();

--##############################################################################
--#                                    End                                     #
--##############################################################################
