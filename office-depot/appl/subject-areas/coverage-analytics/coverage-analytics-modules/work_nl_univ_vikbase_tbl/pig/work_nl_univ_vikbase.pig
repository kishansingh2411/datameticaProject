--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_nl_univ_vikbase                                                 #
--# File                                                                       #
--#     : work_nl_univ_vikbase.pig                                             #
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

SET job.name '$WORK_NL_UNIV_VIKBASE_SSH_ACTION';

work_nl_univ_vikbase = 
	LOAD '$DB_INCOMING.$INCOMING_NL_UNIV_VIKBASE'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_nl_univ_vikbase_generate = 
	FOREACH work_nl_univ_vikbase GENERATE
	RTRIM(code),
	RTRIM(vnum),
	RTRIM(rnum),
	RTRIM(cham),
	RTRIM(naam),
	RTRIM(nama),
	RTRIM(sstr),
	RTRIM(huis),
	RTRIM(hstv),
	RTRIM(post),
	RTRIM(plac),
	RTRIM(pstr),
	RTRIM(pbus),
	RTRIM(posp),
	RTRIM(plap),
	RTRIM(prov),
	RTRIM(hbrn),
	RTRIM(tbrn),
	RTRIM(beco),
	RTRIM(bgrt),
	RTRIM(revo),
	RTRIM(hffi),
	RTRIM(teln),
	RTRIM(tela),
	RTRIM(faxn),
	RTRIM(faxa),
	RTRIM(grow),
	RTRIM(soho),
	RTRIM(recl),
	RTRIM(rere),
	RTRIM(ecac),
	RTRIM(novi),
	RTRIM(fdat),
	RTRIM(inar),
	RTRIM(gemc),
	RTRIM(eggc),
	RTRIM(coro),
	RTRIM(cebu),
	RTRIM(niel),
	RTRIM(inwo),
	RTRIM(impc),
	RTRIM(expc),
	RTRIM(part),
	RTRIM(chab),
	RTRIM(updated),
	RTRIM(filler),
	RTRIM(email),
	RTRIM(wcclass),
	RTRIM(bvg),
	RTRIM(nomail);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_nl_univ_vikbase_generate 
	INTO '$WORK_HDFS/$WORK_NL_UNIV_VIKBASE/batch_id=$batch_id/country_code=$country_code_nl'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################  