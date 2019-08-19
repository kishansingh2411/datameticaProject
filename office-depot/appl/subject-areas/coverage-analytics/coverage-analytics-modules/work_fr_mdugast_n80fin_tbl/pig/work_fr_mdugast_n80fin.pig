--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : work_fr_mdugast_n80fin                                      			   #
--# File                                                                       #
--#     : work_fr_mdugast_n80fin.pig                                               #
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

SET job.name '$WORK_FR_MDUGAST_N80FIN_SSH_ACTION';

work_mugast_n80fin = 
	LOAD '$DB_INCOMING.$INCOMING_FR_MDUGAST_N80FIN'  
	USING org.apache.hive.hcatalog.pig.HCatLoader();

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

work_mugast_n80fin_generate = 
	FOREACH work_mugast_n80fin GENERATE
	RTRIM(siren),
	RTRIM(nic),
	RTRIM(l1_nomen),
	RTRIM(l2_comp),
	RTRIM(l3_cadr),
	RTRIM(l4_voie),
	RTRIM(l5_disp),
	RTRIM(l6_post),
	RTRIM(l7_etrg),
	RTRIM(zr1),
	RTRIM(rpet),
	RTRIM(depet),
	RTRIM(arronet),
	RTRIM(ctonet),
	RTRIM(comet),
	RTRIM(libcom),
	RTRIM(du),
	RTRIM(tu),
	RTRIM(uu),
	RTRIM(codpos),
	RTRIM(zr2),
	RTRIM(tcd),
	RTRIM(zemet),
	RTRIM(codevoie),
	RTRIM(numvoie),
	RTRIM(indrep),
	RTRIM(typevoie),
	RTRIM(libvoie),
	RTRIM(enseigne),
	RTRIM(apet700),
	RTRIM(apet31),
	RTRIM(siege),
	RTRIM(tefet),
	RTRIM(efetcent),
	RTRIM(origine),
	RTRIM(dcret),
	RTRIM(mmintret),
	RTRIM(activnat),
	RTRIM(lieuact),
	RTRIM(actisurf),
	RTRIM(saisonat),
	RTRIM(modet),
	RTRIM(dapet),
	RTRIM(defet),
	RTRIM(explet),
	RTRIM(prodpart),
	RTRIM(auxilt),
	RTRIM(eaeant),
	RTRIM(eaeapet),
	RTRIM(eaesec1t),
	RTRIM(eaesec2t),
	RTRIM(nomen),
	RTRIM(sigle),
	RTRIM(civilite),
	RTRIM(cj),
	RTRIM(tefen),
	RTRIM(efencent),
	RTRIM(apen700),
	RTRIM(apen31),
	RTRIM(aprm),
	RTRIM(tca),
	RTRIM(recme),
	RTRIM(dapen),
	RTRIM(defen),
	RTRIM(dcren),
	RTRIM(mmintren),
	RTRIM(monoact),
	RTRIM(moden),
	RTRIM(explen),
	RTRIM(eaeann),
	RTRIM(eaeapen),
	RTRIM(eaesec1n),
	RTRIM(eaesec2n),
	RTRIM(eaesec3n),
	RTRIM(eaesec4n),
	RTRIM(nbetexpl),
	RTRIM(tcaexp),
	RTRIM(regimp),
	RTRIM(monoreg),
	RTRIM(rpen),
	RTRIM(depcomen),
	RTRIM(vmaj),
	RTRIM(vmaj1),
	RTRIM(vmaj2),
	RTRIM(vmaj3),
	RTRIM(siret),
	RTRIM(maj),
	RTRIM(dmaj),
	RTRIM(eve),
	RTRIM(typetab),
	RTRIM(tel),
	RTRIM(dateve),
	RTRIM(exp_is_actif);

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE work_mugast_n80fin_generate 
	INTO '$WORK_HDFS/$WORK_FR_MDUGAST_N80FIN/batch_id=$batch_id/country_code=$country_code_fr'
	USING PigStorage('\t');

--##############################################################################
--#                                    End                                     #
--##############################################################################