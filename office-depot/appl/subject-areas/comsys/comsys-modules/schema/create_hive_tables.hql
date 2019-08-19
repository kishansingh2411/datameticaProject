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
--#     : Sarfarazkhan , Harshwardhan             		            	 					            #
--#                                                                                     #      
--#######################################################################################

--Creating databases

CREATE DATABASE IF NOT EXISTS ${hiveconf:DB_INCOMING};
use ${hiveconf:DB_INCOMING};
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ODIDTA_GB_SIZBOX}
(
SBSSYS string,
SBORDR string,
SBSUBO string,
SBCRTN string,
SBCTRY string,
SBOCTY string,
SBCUST string,
SBILOC string,
SBOLOC string,
SBSEQB string,
SBSEQR string,
SBCRCK string,
SBCVID string,
SBCARR string,
SBDEPO string,
SBROUT string,
SBCBAR string,
SBCSGT string,
SBCSGC string,
SBTYCT string,
SBSTCT string,
SBLINE string,
SBSIZE string,
SBLINT string,
SBWGTT string,
SBWGTC string,
SBWGMN string,
SBWGMX string,
SBWGTS string,
SBCVAL string,
SBVOLT string,
SBZONE string,
SBLEGO string,
SBLEGA string,
SBFRCK string,
SBSHPB string,
SBSHPD string,
SBOUTF string,
SBTYLB string,
SBSTAT string,
SBPPRI string,
SBPLRQ string,
SBSCHD string,
SBPRTD string,
SBPRTT string,
SBSCAD string,
SBSCAT string,
SBMAND string,
SBMANT string,
SBMANF string,
SBNUML string,
SBSPA1 string,
SBSPA2 string,
SBSPA3 string,
SBSPA4 string,
SBSPA5 string,
SBSPA6 string,
SBSPA7 string,
SBSPA8 string,
SBCRTD string 
)
PARTITIONED BY (batch_id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ODIDTA_GB_SIZBOX}';

-- Table SIZDTL

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ODIDTA_GB_SIZDTL}
(
SDSSYS string,
SDORDR string,
SDSUBO string,
SDLINE string,
SDALIN string,
SDSUBL string,
SDCTRY string,
SDCUST string,
SDSKU string,
SDSKUC string,
SDSKUP string,
SDSKUD string,
SDPICK string,
SDSORT string,
SDCVD1 string,
SDCVD2 string,
SDCVD3 string,
SDCVD4 string,
SDQTPK string,
SDUNPK string,
SDQTMU string,
SDQTCL string,
SDSQCL string,
SDQTIN string,
SDUNIN string,
SDVOLT string,
SDCDDI string,
SDASSM string,
SDWGTC string,
SDWGMN string,
SDWGMX string,
SDCOPT string,
SDCSEQ string,
SDCRTN string,
SDCARR string,
SDSPA1 string,
SDSPA2 string,
SDSPA3 string,
SDSPA4 string,
SDSPA5 string,
SDSPA6 string,
SDSPA7 string,
SDSPA8 string,
SDCRTD string
)
PARTITIONED BY (batch_id  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ODIDTA_GB_SIZDTL}'
tblproperties ("skip.header.line.count"="1");

--Table SIZHED

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ODIDTA_GB_SIZHED}
(
SHSSYS string,
SHORDR string,
SHSUBO string,
SHCTRY string,
SHCUST string,
SHILOC string,
SHINVN string,
SHTRSH string,
SHFSTO string,
SHBOXT string,
SHVOLT string,
SHWGTT string,
SHBOXS string,
SHVOLS string,
SHWGTS string,
SHBOXF string,
SHVOLF string,
SHWGTF string,
SHPALP string,
SHVOLP string,
SHWGTP string,
SHPRTY string,
SHCRTD string,
SHVOLE string,
SHWGTE string,
SHVOLX string,
SHWGTX string
)
PARTITIONED BY (batch_id  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ODIDTA_GB_SIZHED}'
tblproperties ("skip.header.line.count"="1");

-- Table FILHED

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_ODIDTA_GB_FILHED}
(
FHSSYS string,
FHORDR string,
FHSUBO string,
FHOTYP string,
FHOSBT string,
FHCTRY string,
FHOCTY string,
FHCCTY string,
FHBRND string,
FHCHAN string,
FHOSRC string,
FHORIG string,
FHILOC string,
FHOLOC string,
FHCLOC string,
FHDTOR string,
FHCUST string,
FHCUTY string,
FHLANG string,
FHCDVP string,
FHFSTO string,
FHBATN string,
FHBBSN string,
FHBAL1 string,
FHBAL2 string,
FHBAL3 string,
FHBACY string,
FHBACO string,
FHBARG string,
FHBAPC string,
FHBACT string,
FHBPHN string,
FHBEXT string,
FHCDPU string,
FHDTPU string,
FHSERV string,
FHIDSN string,
FHADSQ string,
FHSATN string,
FHSBSN string,
FHSAL1 string,
FHSAL2 string,
FHSAL3 string,
FHSACY string,
FHSACO string,
FHSARG string,
FHSAPC string,
FHSACT string,
FHSPHN string,
FHSEXT string,
FHKEY1 string,
FHKEY2 string,
FHKEY3 string,
FHKEY4 string,
FHOORD string,
FHOSUB string,
FHDTSP string,
FHCOVR string,
FHRKY1 string,
FHRKY2 string,
FHRKY3 string,
FHRKY4 string,
FHPLRQ string,
FHPLCP string,
FHPLHD string,
FHC1PL string,
FHR1PT string,
FHR1PB string,
FHC2PL string,
FHR2PT string,
FHR2PB string,
FHC3PL string,
FHR3PT string,
FHR3PB string,
FHCPPL string,
FHCPPT string,
FHCPPB string,
FHACCT string,
FHCSPH string,
FHINVD string,
FHRHIV string,
FHCURY string,
FHTTDT string,
FHTTMS string,
FHAMTX string,
FHTTTX string,
FHTTIV string,
FHCDTE string,
FHCDDV string,
FHCDIN string,
FHTTCS string,
FHPCUS string,
FHOINV string,
FHMSC1 string,
FHMSC2 string,
FHMSC3 string,
FHMSC4 string,
FHMSC5 string,
FHMSC6 string,
FHLINS string,
FHCDDL string,
FHSLSI string,
FHSLSN string,
FHSTAT string,
FHHLDR string,
FHPPRI string,
FHRUSH string,
FHTMSD string,
FHTMST string,
FHRQUS string,
FHRQPG string,
FHRSDQ string,
FHRSLB string,
FHRPLY string,
FHTRAN string,
FHLOG string,
FHRINV string,
FHSPA1 string,
FHSPA2 string,
FHSPA3 string,
FHSPA4 string,
FHSPA5 string,
FHSPA6 string,
FHSPA7 string,
FHSPA8 string,
FHCRTD string
)
PARTITIONED BY (batch_id  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_ODIDTA_GB_FILHED}'
tblproperties ("skip.header.line.count"="1");


------############## AUDIT LAYER #############-------------------

--# job_statistics #

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_AUDIT}.${hiveconf:TBL_JOB_STATISTIC}
(
  batchId                                        string,
  jobname                                        string,
  tableName                                      string,
  user_ran                                       string,
  startTime                                      string,
  endTime                                        string,
  timeTaken                                      string,
  recordCount                                    string,
  statusCode                                     string,
  recordType                                     string,
  logPath                                        string
)
PARTITIONED BY (current_date  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '${hiveconf:AUDITLOG_HDFS}/${hiveconf:TBL_JOB_STATISTIC}';


--##############################################################################
--#                                    End                                     #
--##############################################################################


