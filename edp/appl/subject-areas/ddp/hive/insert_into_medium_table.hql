set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE  ${hivevar:target_database}.${hivevar:target_table}  
   PARTITION (load_year,load_month)
SELECT * 
 FROM ${hivevar:source_database}.${hivevar:source_table} b
where ${hivevar:sequence_column} not in
(select a.${hivevar:sequence_column} from  ${hivevar:target_database}.${hivevar:target_table} a
 join ${hivevar:source_database}.${hivevar:source_table} bc
   on to_date(a.load_date) = to_date(bc.load_date)
where a.${hivevar:sequence_column} = bc.${hivevar:sequence_column}
)
;


