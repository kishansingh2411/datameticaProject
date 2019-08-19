--#######################################################################################
--#                              General Details                                        #
--#######################################################################################
--#                                                                                     #
--# Name                                                                                #
--#     : click logger                                                                  #
--# File                                                                                #
--#     : clicklogger_daily_job.sh                                                      #
--# Description                                                                         #
--#     : To partiton hive table on daily basis and merging hourl avro files            #   
--#        into day avro file									                          #
--#       	                                                                          #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--#                                                                                     #
--# Author                                                                              #
--#     : SarfarazKhan                                                                  #
--#                                                                                     #
--#######################################################################################

--##############################################################################
--#                                                                            #
--#  Registering required jar files                                            #
--#                                                                            #
--##############################################################################


   REGISTER $pig_lib/avro.jar
   REGISTER $pig_lib/avro-mapred.jar
   REGISTER $pig_lib/jackson-mapper-asl-1.8.8.jar
   REGISTER $pig_lib/jackson-core-asl-1.8.8.jar
   REGISTER $pig_lib/json-simple-1.1.jar
   REGISTER $pig_lib/jython-standalone-2.5.3.jar
   REGISTER $pig_lib/snappy-java-1.0.5.jar


--##############################################################################
--#                                                                            #
--#  Pig will load all hourly data                                             #
--#                                                                            #
--##############################################################################


   avro_data = 
   LOAD '$pig_input/year=$year/month=$month/day=$day/' USING AvroStorage();


--##############################################################################
--#                                                                            #
--#  Pig will Merge all hourly data into day data & Stores                     #
--#                                                                            #
--##############################################################################

     STORE avro_data 
     INTO '$pig_input/year=$year/month=$month/day=$day/hour=24' 
     USING  AvroStorage('same','$pig_input/year=$year/month=$month/day=$day');
     
     
--##############################################################################
--#                                                                            #
--#  						END OF SCRIPT                                      #
--#                                                                            #
--##############################################################################