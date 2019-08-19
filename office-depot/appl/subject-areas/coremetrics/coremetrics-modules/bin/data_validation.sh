#!/bin/sh

#######################################################################################
#                              General Details                               		  #
#######################################################################################
#                                                                            		  #
# Name                                                                                #
#     : clean_up       								   	 		                      #
# File                                                                                #
#     : clean_up.sh                                   	                              #
# Description                                                                         #
#     : Deleting temporary files from batchId directory once data is processed 		  #
#                                                                                     #
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : Deepanshu                         			 					              #
#                                                                                     #
#######################################################################################

# Find absolute path to this script which is used to define module, project, subject area home directory paths.
pushd . > /dev/null
SCRIPT_HOME="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_HOME}" ]); do
    cd "`dirname "${SCRIPT_HOME}"`"
    SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
SCRIPT_HOME="`pwd`";
popd  > /dev/null


# Set module, project, subject area home paths.
MODULE_HOME=`dirname ${SCRIPT_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULES_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECTS_HOME=`dirname ${PROJECT_HOME}`

source $MODULE_HOME/bin/coremetrics_functions.sh
source $MODULE_HOME/etc/coremetrics-modules.properties

currentDate=`date +%Y%m%d`
#currentDate=20150502
batchId=`cat $COREMETRICS_PROPERTIES_SUBJECT_AREA_HOME$BATCH_ID_FILE`
#batchId=20150707021859
## hive table validation###

hive -e "use incoming_clickstream;select count(order_id) from incoming_order where batch_id =$batchId ; select count(distinct session_id) from incoming_page_view where batch_id = $batchId; select count(session_id) from incoming_page_view  where batch_id = $batchId; select sum(order_total) from incoming_order where batch_id=$batchId; select count(distinct session_id) from incoming_cart_item_addition where  batch_id =$batchId; select count(distinct session_id) from incoming_cart_item_abandonment where  batch_id =$batchId; select count(distinct session_id) from incoming_product_view where  batch_id =$batchId; " >>$PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_temp_result.txt"

## Hbase table validation ###  

echo "select SUM(TOTAL_ORDERS),day from CLICKSTREAM_VIEW_DAILY where day = $currentDate group by day;" >> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select sum(TOTAL_VISITS),day from CLICKSTREAM_VIEW_DAILY where day = $currentDate group by day;" >> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select sum(TOTAL_PAGE_VIEWS),day from CLICKSTREAM_VIEW_DAILY where day = $currentDate group by day;" >> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select sum(TOTAL_REV_FRM_NCUST +TOTAL_REV_FRM_ECUST + TOTAL_REV_FRM_ANYMCUST),day from CLICKSTREAM_VIEW_DAILY  where day = $currentDate group by day;" >> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select sum(TOTAL_CART_STARTED),day from CLICKSTREAM_VIEW_DAILY where day= $currentDate group by day;">> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select sum(total_cart_aband),day from CLICKSTREAM_VIEW_DAILY where day= $currentDate group by day;">> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select sum(total_view_products),day from CLICKSTREAM_VIEW_DAILY where day= $currentDate group by day;">> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql

cd $PHOENIX_HOME
 ./psql.py localhost:2181 $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql |grep "$currentDate" | awk '{ print $1 }' | sed 's/,//g' >> $PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_temp_result.txt"
#./psql.py localhost:2181 $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql >> $PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_temp_result.txt"

hive_file=`cat $PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_temp_result.txt"`
	for i in "${hive_file[@]}"
	 do
	   data_list=$i
	 done
	 
echo $data_list >$PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_result.txt"
echo "$PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_result.txt""
## copy validation file from local directory to hdfs #####
echo "copy from  ***  $PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_result.txt""
echo "copy to *** $AUDITLOG_HDFS/$VALIDATION/$batchId/"
hadoop fs -mkdir $AUDITLOG_HDFS/$VALIDATION/"batch_id="$batchId/
hadoop fs -copyFromLocal $PROJECTS_HOME$PROJ_TMP/$batchId/$batchId"_validation_result.txt" $AUDITLOG_HDFS/$VALIDATION/"batch_id="$batchId/

hadoop fs -chmod -R 777 $HDFS_PREFIX > /dev/null
hive -e "use $DB_AUDIT; alter table $VALIDATION add partition(batch_id=$batchId);"


