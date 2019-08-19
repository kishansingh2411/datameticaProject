--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_ir_univ_billmoss                                                #
--# File                                                                       #
--#     : work_ir_univ_billmoss.pig                                            #
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

SET job.name '$WORK_IR_UNIV_BILLMOSS_SSH_ACTION';

work_ir_univ_billmoss = 
	LOAD '$DB_INCOMING.$INCOMING_IR_UNIV_BILLMOSS'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_ir_univ_billmoss_generate = 
	FOREACH work_ir_univ_billmoss GENERATE
	RTRIM(siteid),
	RTRIM(site_name),
	RTRIM(add1),
	RTRIM(add2),
	RTRIM(add3),
	RTRIM(add4),
	RTRIM(add5),
	RTRIM(add6),
	RTRIM(employees),
	RTRIM(sic1987_4digit),
	RTRIM(co_uid),
	RTRIM(suspend);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_ir_univ_billmoss_generate 
	INTO '$WORK_HDFS/$WORK_IR_UNIV_BILLMOSS/batch_id=$batch_id/country_code=$country_code_ir'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################   