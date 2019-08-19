source $1/office-depot/appl/bin/test/test_functions $1

SOURCE_HOME=$1

#####Create test tables##############

function fn_create_hive_tbl

####Creating data directory & moving sample files for comparing with final output file into HDFS for work layer###########

for work_tbl_name in ${ARRAY_WORK_RAW_TBLS[@]}
do

fn_hdp_mkdir "$TEST_WORK_HDFS/$work_tbl_name$DATA_DIR"

src_file="$SOURCE_HOME$COREMETRICS_MODULES/$work_tbl_name$TEST_DIR$DATA_DIR/$work_tbl_name"".txt"
dest_file="$TEST_WORK_HDFS/$work_tbl_name$DATA_DIR/"

fn_hdp_put $src_file $dest_file

done

####Move sample files for comparing with final output file into HDFS for outgoing layer###########

fn_hdp_mkdir "$TEST_OUTGOING_HDFS/$HBASE_TBL$DATA_DIR"

src_file_outoing="$SOURCE_HOME$COREMETRICS_MODULES/$HBASE_TBL$TEST_DIR$DATA_DIR/$HBASE_TBL"".txt"
dest_file_outgoing="$TEST_OUTGOING_HDFS/$HBASE_TBL$DATA_DIR/"

fn_hdp_put $src_file_outgoing  $dest_file_outgoing


#####Populate sample data into incoming tables##############

for value in ${ARRAY_INCOMING_RAW_TBLS[@]}
do

fn_move_to_incoming $value $SOURCE_HOME

done 

#####Populate data into gold tables############

for table_name_gold in ${ARRAY_GOLD_TBLS[@]}
do

fn_pig_wrapper $table_name_gold $SOURCE_HOME

done


#####Populate data into outgoing file############


fn_pig_wrapper $HBASE_TBL  $SOURCE_HOME


#####Call wutz scripts########





