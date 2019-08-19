--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_cdmprddta_dm_country                                            #
--# File                                                                       #
--#     : work_cdmprddta_dm_country.pig                                        #
--# Description                                                                #
--#     : To load data into work layer                                         #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Sonali Rawool                                                        #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################
Register $OPENCSV_JAR;
SET job.name '$WORK_CDMPRDDTA_DM_COUNTRY_SSH_ACTION';
work_cdmprddta_dm_country = LOAD '$DB_INCOMING.$INCOMING_CDMPRDDTA_DM_COUNTRY'
                          USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_cdmprddta_dm_country_generate = FOREACH work_cdmprddta_dm_country GENERATE 
                      RTRIM(country_cd),
                      RTRIM(country_nm);
                      

--##############################################################################
--#                                   Store                                    #
--##############################################################################
STORE work_cdmprddta_dm_country_generate INTO '$WORK_HDFS/$WORK_CDMPRDDTA_DM_COUNTRY/batch_id=$batch_id/'
USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################

         