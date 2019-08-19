--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : outgoing_merged                                      				   #
--# File                                                                       #
--#     : outgoing_merged.pig                                  				   #
--# Description                                                                #
--#     : To load data into outgoing layer                                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : kishan singh                                                         #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

SET job.name '$MANAGE_OUTGOING_SCHEMA_SSH_ACTION';
SET default_parallel 2;

-- Loading data from outgoing table.

outgoing_at_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_at/part-*' using PigStorage(',');
outgoing_be_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_be/part-*' using PigStorage(',');
outgoing_fr_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_fr/part-*' using PigStorage(',');
outgoing_ge_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_ge/part-*' using PigStorage(',');
outgoing_uk_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_uk/part-*' using PigStorage(',');
outgoing_ir_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_ir/part-*' using PigStorage(',');
outgoing_it_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_it/part-*' using PigStorage(',');
outgoing_nl_tbl = LOAD '$OUTPUT_DIRECTORY/$record_type/$batch_id/$country_code_nl/part-*' using PigStorage(',');  

--##############################################################################
--#                                 Transform                                  #
--##############################################################################

-- Filtering data for current batch_id.

outgoing_union_table= UNION outgoing_at_tbl,outgoing_be_tbl,outgoing_fr_tbl,outgoing_ge_tbl,outgoing_uk_tbl,outgoing_ir_tbl,outgoing_it_tbl,outgoing_nl_tbl;

--##############################################################################
--#                                   Store                                    #
--##############################################################################

-- Storing records into /office_depot/coverage_analytics/outgoing/daily/batch_id/country_cd directory.

STORE outgoing_union_table INTO '$OUTPUT_DIRECTORY/$record_type/$batch_id/$MERGED_OUTPUT' USING PigStorage('\t');

--##############################################################################t
--#                                    End                                     #
--##############################################################################

         