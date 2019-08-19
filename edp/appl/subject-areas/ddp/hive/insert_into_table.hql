set hive.exec.dynamic.partition.mode=nonstrict
;

INSERT INTO TABLE 
   ${hivevar:target_database}.${hivevar:table_prefix}${hivevar:target_table} 
   PARTITION (load_year,load_month)
SELECT 
   * 
FROM ${hivevar:source_database}.${hivevar:table_prefix}${hivevar:source_table}
;