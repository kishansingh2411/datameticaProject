--#######################################################################################
--#                              General Details                               		    #
--#######################################################################################
--#                                                                            		    #
--# Name                                                                                #
--#     : create_hive_tables      								   	 		            #
--# File                                                                                #
--#     : create_hive_tables.hql                                   	                    #
--# Description                                                                         #
--#     : Contains ddl for Hive Schema 		  									        #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--# Author                                                                              #
--#     : Sarfarazkhan                			 					                    #
--#                                                                                     #
--#######################################################################################


--Creating databases

CREATE DATABASE IF NOT EXISTS click_logger;



--# Creating incoming layer tables 

CREATE EXTERNAL TABLE IF NOT EXISTS clickstream_logger

PARTITIONED BY (year int , month int , day int , hour int)

ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'

STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'

OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'

LOCATION '$avro_incoming_dir'

TBLPROPERTIES ('avro.schema.url'=' hdfs://$avro_scehma_path/$avro_schema_file');


--##############################################################################
--#                                    End                                     #
--##############################################################################
