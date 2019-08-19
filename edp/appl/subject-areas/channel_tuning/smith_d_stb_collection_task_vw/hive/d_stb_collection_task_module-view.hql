CREATE OR REPLACE VIEW 
 smith_channel_tuning.d_stb_collection_task AS
SELECT 
  '1% Sample' AS sample, 
  a.stb_collection_task_id, 
  a.collection_task_identification, 
  a.dtm_to_start, 
  a.dtm_to_finish, 
  a.dtm_task_published, 
  a.dtm_task_suspended, 
  a.dtm_created, 
  a.dtm_last_updated, 
  a.sourced_from_system, 
  a.last_updated_by_system, 
  a.aggregator_string, 
  a.pre_condition_string, 
  a.param_string, 
  '01. One Percent of Plant (Prod 25K HH)' AS sample_descr, 
  CASE WHEN a.stb_collection_task_id = -2 THEN 1 ELSE a.sample_id END AS sample_id
FROM 
 smith_channel_tuning.smith_t_d_stb_collection_task a
WHERE 
  a.sample_id = 1 OR a.stb_collection_task_id = -2
UNION ALL
SELECT 
  '100% Sample' AS sample, 
  b.stb_collection_task_id, 
  b.collection_task_identification, 
  b.dtm_to_start, 
  b.dtm_to_finish, 
  b.dtm_task_published, 
  b.dtm_task_suspended, 
  b.dtm_created, 
  b.dtm_last_updated, 
  b.sourced_from_system, 
  b.last_updated_by_system, 
  b.aggregator_string, 
  b.pre_condition_string, 
  b.param_string, 
  '20. Hundred Percent of Plant' AS sample_descr,
  CASE WHEN b.stb_collection_task_id = -2 THEN 20 ELSE b.sample_id END AS sample_id
FROM smith_channel_tuning.smith_t_d_stb_collection_task b
WHERE 
  b.sample_id = 1 
  OR b.sample_id = 20 
  OR b.sample_id = 21 
  OR b.sample_id = 22 
  OR b.stb_collection_task_id = -2
  UNION ALL
  SELECT 
     '1% Sample A' AS sample, 
     c.stb_collection_task_id, 
     c.collection_task_identification, 
     c.dtm_to_start, 
     c.dtm_to_finish, 
     c.dtm_task_published, 
     c.dtm_task_suspended, 
     c.dtm_created, 
     c.dtm_last_updated, 
     c.sourced_from_system, 
     c.last_updated_by_system, 
     c.aggregator_string, 
     c.pre_condition_string, 
     c.param_string, 
     'ACTIVITY_1PCT 1HR POSTBACK' AS sample_descr,
     CASE WHEN c.stb_collection_task_id = -2 THEN 21 ELSE c.sample_id END AS sample_id
  FROM smith_channel_tuning.smith_t_d_stb_collection_task c
  WHERE c.sample_id = 21 OR c.stb_collection_task_id = -2
  UNION ALL
SELECT 
   '1% Sample B' AS sample, 
   d.stb_collection_task_id, 
   d.collection_task_identification, 
   d.dtm_to_start, 
   d.dtm_to_finish, 
   d.dtm_task_published, 
   d.dtm_task_suspended, 
   d.dtm_created, 
   d.dtm_last_updated, 
   d.sourced_from_system, 
   d.last_updated_by_system, 
   d.aggregator_string, 
   d.pre_condition_string, 
   d.param_string, 
   'GUARANTEE_1PCT 1 HR POSTBACK' AS sample_descr,
   CASE WHEN d.stb_collection_task_id = -2 THEN 22 ELSE d.sample_id END AS sample_id   
FROM smith_channel_tuning.smith_t_d_stb_collection_task d  
WHERE d.sample_id = 22 OR d.stb_collection_task_id = -2
;