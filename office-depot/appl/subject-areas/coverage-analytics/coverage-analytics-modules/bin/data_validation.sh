#!/bin/sh

#######################################################################################
#                              General Details                               		  #
#######################################################################################
#                                                                            		  #
# Name                                                                                #
#     : validation script							   	 		                      #
# File                                                                                #
#     : data_validation.sh                             	                              #
# Description                                                                         #
#     : Data Validation                                                          	  #
#                                                                                     #
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : Shweta                           			 					              #
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

source $MODULE_HOME/bin/coverageAnalytics_functions.sh

#currentDate=`date +%Y%m%d`
currentDate=`date +%Y%m`

#currentDate=20150502
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
#batchId=20150707021859

## Validation from Source FTP data to incoming files:

#for i in "${ARRAY_INCOMING_RAW_TBLS[@]}"
#do		
#    table_name=$i	    
#    dirname=$table_name"_tbl"	
#    count_$i=`wc -l $PROJECTS_HOME$PROJ_TMP/$batchId$STAGING/$table_name.csv`
#    hive -e "use $DB_INCOMING;select count(*) from $table_name "	
#done

## hive table validation###

hive -e "use $DB_INCOMING;select COUNT(DISTINCT id)  from gold_uk_ir_fr_data group by country_code and batch_id = $batchId ; 
						  select SUM( ext_selling_price_amt)  from gold_dm_transaction_dtl group by country_cd and batch_id = $batchId; 
						  select COUNT(DISTINCT associate_id), COUNT(DISTINCT customer_id) from gold_am_assigned_customer group by country_cd having end_dt = '12/31/9999' and batch_id=$batchId" ; 
						  
## Hbase table validation ###  

echo "select total_sales_represntatives,month from officedepot_coverage_analytics_view_monthly where month = $currentDate group by month;" >> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select total_prospects,month from CLICKSTREAM_VIEW_DAILY where month = $currentDate group by month;" >> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select total_accounts,month from CLICKSTREAM_VIEW_DAILY where month = $currentDate group by month;" >> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql
echo "select total_revenue,month from CLICKSTREAM_VIEW_DAILY where month= $currentDate group by month;">> $PROJECTS_HOME$PROJ_TMP/$batchId/validation.sql

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


